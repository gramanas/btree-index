/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include <iostream>
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
//#define DEBUG

namespace badgerdb
{

  /////////////
  // Private //
  /////////////
  Str BTreeIndex::makeIndexFilename(const Str &relationName,
                                const int arrByteOffset) {
    std::ostringstream idxStr;
    idxStr << relationName << '.' << arrByteOffset;
    return idxStr.str();
  }

  bool BTreeIndex::verifyFile(const Str &relName) {
    Page *h;
    // read page
    p_bufMgr->readPage(p_file, m_headerPageNum, h);
    // cast to struct
    IndexMetaInfo IMF = *(reinterpret_cast<const IndexMetaInfo*>(getPageNode(h).data()));
    p_bufMgr->unPinPage(p_file, m_headerPageNum, false);
    // if those are not the same the file doesn't match the index we are trying to create
    if (m_attrByteOffset != IMF.attrByteOffset ||
        m_attributeType  != IMF.attrType ||
        strcmp(relName.c_str(), IMF.relationName) != 0) {
      return false;
    }
    return true;
  }

  void BTreeIndex::injectIndexMetadata(const Str &relName) {
    // create the struct
    IndexMetaInfo IMF;
    strcpy(IMF.relationName, relName.c_str());
    IMF.attrByteOffset = m_attrByteOffset;
    IMF.attrType = m_attributeType;
    IMF.rootPageNo = 2; // rules say so
    // cast it to a string
    std::string metadata(reinterpret_cast<char*>(&IMF), sizeof(IMF));

    // allocate a page and write on it.
    PageId pNo;
    Page *h;
    p_bufMgr->allocPage(p_file, pNo, h);
    h->insertRecord(metadata);
    p_file->writePage(pNo, *h);
    p_bufMgr->unPinPage(p_file, pNo, true);
    //p_bufMgr->flushFile(p_file);
  }

  PageId BTreeIndex::getRootPageId() {
    Page *h;
    p_bufMgr->readPage(p_file, m_headerPageNum, h);
    IndexMetaInfo IMF = *(reinterpret_cast<const IndexMetaInfo*>(getPageNode(h).data()));
    p_bufMgr->unPinPage(p_file, m_headerPageNum, false);
    return IMF.rootPageNo;
  }

  void BTreeIndex::setRootPage(PageId &pId) {
    Page *h;
    p_bufMgr->readPage(p_file, m_headerPageNum, h);
    IndexMetaInfo IMF = *(reinterpret_cast<const IndexMetaInfo*>(getPageNode(h).data()));
    IMF.rootPageNo = pId;
    std::string metadata(reinterpret_cast<char*>(&IMF), sizeof(IMF));

    h->updateRecord(h->begin().getCurrentRecord(), metadata);
    p_file->writePage(pId, *h);
    p_bufMgr->unPinPage(p_file, m_headerPageNum, true);
    //p_bufMgr->flushFile(p_file);
  }

  PageId BTreeIndex::insertNode() {
    PageId pNo;
    Page *pageNode;
    p_bufMgr->allocPage(p_file, pNo, pageNode);
    p_bufMgr->unPinPage(p_file, pNo, false);
    return pNo;
  }

  template <class T>
  void BTreeIndex::writePage(T &st, PageId pId) {
    std::string rootNodeMD(reinterpret_cast<char*>(&st), sizeof(st));
    Page *p;

    p_bufMgr->readPage(p_file, pId, p);
    p->insertRecord(rootNodeMD);
    p_file->writePage(pId, *p);
    p_bufMgr->unPinPage(p_file, pId, true);
    //p_bufMgr->flushFile(p_file);
  }

  NonLeafNodeInt BTreeIndex::getNodeFromPageId(const PageId pId) {
    Page *page;
    p_bufMgr->readPage(p_file, pId, page);
    NonLeafNodeInt node = *(reinterpret_cast<const NonLeafNodeInt*>(getPageNode(page).data()));
    p_bufMgr->unPinPage(p_file, pId, false);
    return node;
  }

  LeafNodeInt BTreeIndex::getLeafNodeFromPageId(const PageId pId) {
    Page *page;
    if (pId == 0) {
      throw;
      return LeafNodeInt();
    }
    p_bufMgr->readPage(p_file, pId, page);
    LeafNodeInt leaf = *(reinterpret_cast<const LeafNodeInt*>(getPageNode(page).data()));
    p_bufMgr->unPinPage(p_file, pId, false);
    return leaf;
  }

  bool BTreeIndex::search(const int key, LeafNodeInt &ret) {
    return treeSearch(key, getNodeFromPageId(getRootPageId()), ret);
  }

  int BTreeIndex::getKeyPosition(const int key, int *arr) {
    int pos = 0;
    for (int i = 0; i < m_nodeOccupancy; i++) {
      if (arr[i] == -1) {
        continue;
      }
      if (key >= arr[i]) {
        pos = i + 1;
      }
    }
    return pos;
  }

  bool BTreeIndex::treeSearch(const int key, NonLeafNodeInt NLN, LeafNodeInt &ret) {
    int pos = getKeyPosition(key, NLN.keyArray);
    if (NLN.level == 1) {
      // we are above the leafs
      LeafNodeInt LNInt;
      try {
        LNInt = getLeafNodeFromPageId(NLN.pageNoArray[pos]);
      } catch (InvalidPageException e) {
        PageId newPage = insertNode();
        LNInt = LeafNodeInt();
        writePage(LNInt, newPage);
        ret = LNInt;
        return false;
      }
      ret = LNInt;
      return true;
    }
    return treeSearch(key, getNodeFromPageId(NLN.pageNoArray[pos]), ret);
  }

  ////////////
  // Public //
  ////////////

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const Str & relationName,
                Str & outIndexName,
                BufMgr *bufMgrIn,
                const int attrByteOffset,
                const Datatype attrType)
  : p_bufMgr(bufMgrIn),
    m_headerPageNum(1),
    m_attributeType(attrType),
    m_attrByteOffset(attrByteOffset),
    m_leafOccupancy(0),
    m_nodeOccupancy(0),
    m_scanExecuting(false),
    m_nextEntry(0),
    m_currentPageNum(PageId()),
    p_currentPageData(nullptr) {

  switch (m_attributeType) {
  case INTEGER:
    m_leafOccupancy = INTARRAYLEAFSIZE;
    m_nodeOccupancy = INTARRAYNONLEAFSIZE;
    break;
  case DOUBLE:
    // not supported
    break;
  case STRING:
    // not supported
    break;
  }

  outIndexName = makeIndexFilename(relationName, attrByteOffset);
  try {
    p_file = new BlobFile(outIndexName, false); // don't create new
    if (!verifyFile(relationName)) {
      throw BadIndexInfoException("File contains different metadata that the ones provided.");
      return;
    }
  } catch (FileNotFoundException e) {
    p_file = new BlobFile(outIndexName, true); // create new
    injectIndexMetadata(relationName);
    // insert the rest
    FileScan fs = FileScan(relationName, p_bufMgr);
    try {
      RecordId scanRid;
      while(1) {
        fs.scanNext(scanRid);
        const char *record = fs.getRecord().c_str();
        void *key = (void *)(record + m_attrByteOffset);
        insertEntry(key, scanRid);
      }
    } catch(EndOfFileException e) {
    }
  }
  p_bufMgr->flushFile(p_file);
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
  switch (m_attributeType) {
  case INTEGER:
    break;
  case DOUBLE:
    break;
  case STRING:
    break;
  }
        /* YOUR CODE HERE */
}




// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid)  {
  Page *p;
  PageId rootId;
  if (isInitialized) {
    rootId = getRootPageId();
    // open root page
    p_bufMgr->readPage(p_file, rootId, p);
    NonLeafNodeInt NLNInt;
    NLNInt = *(reinterpret_cast<const NonLeafNodeInt*>(getPageNode(p).data()));
    p_bufMgr->unPinPage(p_file, rootId, false);
  } else {
    // There is no root page yet.
    // make it
    rootId = insertNode();
    NonLeafNodeInt NLNInt = NonLeafNodeInt();
    NLNInt.keyArray[0] = *(int *)key;
    NLNInt.level = 1;
    PageId leafId = insertNode();
    NLNInt.pageNoArray[0] = leafId;
    writePage(NLNInt, rootId);
    setRootPage(rootId);

    // now make the first leaf
    LeafNodeInt LNInt = LeafNodeInt();
    LNInt.keyArray[0] = *(int *)key;
    LNInt.ridArray[0] = rid;
    LNInt.rightSibPageNo = PageId();
    writePage(LNInt, leafId );
    isInitialized = true;
    return;
  }

  // insert the current key
  LeafNodeInt LNInt;
  if (search(*(int *)key, LNInt)) {
    int numOfKeys = 0;
    for (int i = 0; i < m_leafOccupancy; i++) {
      if (LNInt.keyArray[i] != -1) {
        numOfKeys++;
      }
    }
    if (numOfKeys < m_leafOccupancy - 1) {
      // add key here
    }
    
  }

  for (int i = 0; i < m_nodeOccupancy; i++) {
    
  }

}





// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
                                   const Operator lowOpParm,
                                   const void* highValParm,
                                   const Operator highOpParm) {
        /* YOUR CODE HERE */

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) {

        /* YOUR CODE HERE */

}


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() {

        /* YOUR CODE HERE */

}

}
