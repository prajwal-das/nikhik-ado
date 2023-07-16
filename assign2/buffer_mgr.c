#include <string.h>
#include <stdlib.h>
#include "dberror.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"


int readsCnt, writeCnt;

typedef struct CacheRequiredInfo {
    SM_FileHandle *fileHandlerPntr;
    int writeCnt;
    char *bufferSize;
    int readsCnt;
    Queue *queuePointer;
} CacheRequiredInfo;


CacheRequiredInfo *initCacheRequiredInfo(BM_BufferPool *const buffManager) {
    CacheRequiredInfo *cacheRequiredInfo = malloc(sizeof(CacheRequiredInfo));
    cacheRequiredInfo->readsCnt = 0;
    readsCnt = 0;
    cacheRequiredInfo->fileHandlerPntr = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
    cacheRequiredInfo->writeCnt = 0;
    writeCnt = 0;
}

RC deQueue(BM_BufferPool *const buffManager) {
    int counter = 0;
    pageInfo *pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;

    loop:
    if (counter == (((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes - 1))
        (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail = pginformation;
    else
        pginformation = (*pginformation).nextPageInfo;

    counter++;

    if (counter < (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).filledframes)
        goto loop;

    // Setting temp variables
    int tailnum;
    int pageDelete = 0;
    pageInfo *pinfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail;
    counter = 0;

    while (counter < ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->totalNumOfFrames) {
        counter++;
        if ((pinfo->fixCount) == 0) {
            pageDelete = (*pinfo).pageNum;
            if ((*pinfo).pageNum != ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->pageNum) {
                (*pinfo).nextPageInfo->prevPageInfo = (*pinfo).prevPageInfo;
                (*pinfo).prevPageInfo->nextPageInfo = (*pinfo).nextPageInfo;
            } else {
                (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail = (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail->prevPageInfo;
                (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail->nextPageInfo = NULL;
            }
        } else {
            tailnum = (*pinfo).pageNum;
            pinfo = (*pinfo).prevPageInfo;
        }
    }

    if ((*pinfo).dirtyPage == 1) {
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
        cache->writeCnt++;
        writeCnt++;
        writeBlock(pinfo->pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr, pinfo->bufferData);
    }
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes--;

    if (tailnum == ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->pageNum)
        return 0;

    return pageDelete;
}

RC Enq_pageFragme(BM_PageHandle *const page, const PageNumber pageNum, BM_BufferPool *const buffManager) {
    pageInfo *newpginformation = (pageInfo *) malloc(sizeof(pageInfo));

    (*newpginformation).frameNum = 0;
    (*newpginformation).pageNum = pageNum;
    (*newpginformation).fixCount = 1;
    (*newpginformation).dirtyPage = 0;
    (*newpginformation).prevPageInfo = NULL;
    (*newpginformation).nextPageInfo = NULL;
    char *c = (char *) malloc(PAGE_SIZE * sizeof(char));
    (*newpginformation).bufferData = c;


    pageInfo *pginformation = newpginformation;

    int pgDel_counter = -1;

    if (!((*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).filledframes !=
          (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).totalNumOfFrames)) { // check if frames are full. If full remove pages
        pgDel_counter = deQueue(buffManager);
    }

    if ((*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).filledframes != 0) {
        readBlock(pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                  pginformation->bufferData);    // read block of data
        if (pgDel_counter == -1)
            pginformation->frameNum = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head->frameNum + 1;
        else
            pginformation->frameNum = pgDel_counter;
        page->data = pginformation->bufferData;
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
        cache->readsCnt++;
        readsCnt++;
        pginformation->nextPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
        ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head->prevPageInfo = pginformation;
        ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head = pginformation;
        page->pageNum = pageNum;
    } else {
        readBlock((*pginformation).pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                  (*pginformation).bufferData);
        (*page).data = (*pginformation).bufferData;
        (*pginformation).pageNum = pageNum;
        (*page).pageNum = pageNum;
        (*pginformation).nextPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
        ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head->prevPageInfo = pginformation;
        (*pginformation).frameNum = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head->frameNum;
        ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head = pginformation;
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
        cache->readsCnt++;
        readsCnt++;
    }
    (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).filledframes++;

    return RC_OK;
}

RC LRUpin(BM_BufferPool *const buffManager, BM_PageHandle *const page, const PageNumber pageNum) {
    pageInfo *pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    int pageFragment = 0;
    int temp = 0;
    // Here we are finding the node for the input pagenumber
    loop:
    if (pageFragment == 0) {
        if ((*pginformation).pageNum == pageNum) {
            pageFragment = 1;
        } else {
            pginformation = (*pginformation).nextPageInfo;
        }
    }
    temp++;
    if (temp < buffManager->numPages) {
        goto loop;
    }

    if (pageFragment == 1) {
        (*page).pageNum = pageNum;
        (*page).data = (*pginformation).bufferData;
        (*pginformation).fixCount++;

        if (pginformation != (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head) {
            (*pginformation).prevPageInfo->nextPageInfo = (*pginformation).nextPageInfo;
        } else {
            (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head = pginformation;
            (*pginformation).nextPageInfo = (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head;
            (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head->prevPageInfo = pginformation;
        }

        if ((*pginformation).nextPageInfo &&
            pginformation == (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head) {
            return 0;
        } else if (pginformation != ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head &&
                   (*pginformation).nextPageInfo) {
            (*pginformation).nextPageInfo->prevPageInfo = (*pginformation).prevPageInfo;
            if (pginformation == (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail) {
                (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail = (*pginformation).prevPageInfo;
                ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->nextPageInfo = NULL;
                (*pginformation).prevPageInfo = NULL;
                (*pginformation).nextPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
                (*pginformation).nextPageInfo->prevPageInfo = pginformation;
                (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head = pginformation;
            } else {
                (*pginformation).prevPageInfo = NULL;
                (*pginformation).nextPageInfo = (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head;
                (*pginformation).nextPageInfo->prevPageInfo = pginformation;
                (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head = pginformation;
            }
        }

    } else if (pageFragment == 0)
        Enq_pageFragme(page, pageNum, buffManager);
    return RC_OK;
}

RC FIFOpin(BM_BufferPool *const buffManager, BM_PageHandle *const page, const PageNumber pageNum) {
    int numPages = buffManager->numPages;
    int pageFound = 0;

    pageInfo *pgList = NULL;
    pgList = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    pageInfo *pginformation = NULL;

    // Here we are finding the node for the input pagenumber
    int temp = 0;
    do {
        if (pageFound == 0) {
            if (pgList->pageNum == pageNum) {
                pageFound = 1;

            } else {
                pgList = pgList->nextPageInfo;
            }

        }
        temp++;
    } while (temp < numPages);


    if (pageFound == 1) {
        pgList->fixCount++;
        page->data = pgList->bufferData;
        page->pageNum = pageNum;

        return RC_OK;

        //break;
    }

    pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    int returncode = -1;

    do {
        if (pginformation->pageNum == -1) {
            (*pginformation).fixCount = 1;
            (*pginformation).dirtyPage = 0;
            (*pginformation).pageNum = pageNum;
            (*page).pageNum = pageNum;

            (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).filledframes =
                    (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).filledframes + 1;

            readBlock((*pginformation).pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                      (*pginformation).bufferData);

            (*page).data = (*pginformation).bufferData;
            CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
            cache->readsCnt++;
            readsCnt++;
            returncode = 0;
            break;
        } else {
            pginformation = pginformation->nextPageInfo;
        }

    } while (((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes <
             ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->totalNumOfFrames);

    if (returncode == 0) {
        return RC_OK;

    }


    // Creating new node
    pageInfo *addnode = (pageInfo *) malloc(sizeof(pageInfo));
    addnode->dirtyPage = 0;
    addnode->fixCount = 1;
    addnode->bufferData = NULL;
    addnode->pageNum = pageNum;
    addnode->nextPageInfo = NULL;
    page->pageNum = pageNum;
    addnode->prevPageInfo = (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail;
    pginformation = (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head;

    temp = 0;
    bool flag = true;
    loop:
    if ((*pginformation).fixCount != 0) {
        pginformation = (*pginformation).nextPageInfo;
    } else {
        flag = false;
    }
    if (flag)
        temp++;

    if (temp < numPages && flag)
        goto loop;

    pageInfo *page_info1 = NULL;
    page_info1 = pginformation;


    if (pginformation != ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head &&
        pginformation != ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail) {
        (*pginformation).nextPageInfo->prevPageInfo = (*pginformation).prevPageInfo;
        (*pginformation).prevPageInfo->nextPageInfo = (*pginformation).nextPageInfo;
    }

    if (temp == numPages) {
        return RC_NO_FREESPACE_ERROR;
    }

    switch (pginformation != ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head &&
            pginformation != ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail) {
        case true:
            pginformation->prevPageInfo->nextPageInfo = pginformation->nextPageInfo;
            pginformation->nextPageInfo->prevPageInfo = pginformation->prevPageInfo;
            break;
    }

    if ((*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head == pginformation) {
        (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head = (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head->nextPageInfo;
        (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head->prevPageInfo = NULL;
    }

    switch (pginformation == ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail) {
        case true:
            ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail = pginformation->prevPageInfo;
            addnode->prevPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail;
            break;
    }

    // If page is dirty, we write back to the disk

    if ((*page_info1).dirtyPage == 1) {
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
        cache->writeCnt++;
        writeCnt++;
        writeBlock(page_info1->pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr, page_info1->bufferData);
    }

    (*addnode).frameNum = page_info1->frameNum;
    (*addnode).bufferData = page_info1->bufferData;
    readBlock(pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr, (*addnode).bufferData);
    (*page).data = (*addnode).bufferData;
    CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
    cache->readsCnt++;
    readsCnt++;
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->nextPageInfo = addnode;
    (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail = addnode;

    return RC_OK;
}


//  --- main


//void addNode(Queue *bufferList, pageInfo *node, int position) {
//    bufferList->tail->nextPageInfo = node;
//    bufferList->tail->nextPageInfo->nextPageInfo = bufferList->tail;
//    bufferList->tail = bufferList->tail->nextPageInfo;
//    bufferList->tail->frameNum = position;
//}

RC checkBufManger(BM_BufferPool *bufferManager) {
    RC valRes = RC_OK;
    if(!bufferManager) {
        valRes = RC_BUFFER_POOL_NOTFOUND;
    }
    if(bufferManager->mgmtData == NULL) {
        valRes = RC_BUFFER_POOL_NOTFOUND;
    }
    return valRes;
}

pageInfo *createNewPageInfo(int position) {
    pageInfo *pinf = calloc(PAGE_SIZE, sizeof(pageInfo));
    pinf->frameNum = position;
    pinf->dirtyPage = 0;
    pinf->bufferData = (char *) calloc(PAGE_SIZE, sizeof(char));
    pinf->pageNum = -1;
    pinf->fixCount = 0;
    return pinf;
}

RC initBufferPool(BM_BufferPool *const buffManager, const char *const pageFileName, const int numPages,
                  ReplacementStrategy strategy, void *stratData) {

    char *buffer_Size = (char *) calloc(numPages, sizeof(char) * PAGE_SIZE);

    CacheRequiredInfo *cacheRequiredInfo = initCacheRequiredInfo(buffManager);
    cacheRequiredInfo->queuePointer = (Queue *) malloc(sizeof(Queue));
    cacheRequiredInfo->bufferSize = buffer_Size;
    (*buffManager).mgmtData = cacheRequiredInfo;


    pageInfo *pageInfoArray[numPages];

    for (int curPage = 0; curPage < numPages; curPage++) {
        pageInfoArray[curPage] = createNewPageInfo(curPage);
    }

    for (int curPage = 0; curPage < numPages; curPage++) {
        pageInfoArray[curPage]->nextPageInfo = pageInfoArray[curPage + 1];
        pageInfoArray[curPage]->prevPageInfo = pageInfoArray[curPage - 1];
    }
    openPageFile((char *) pageFileName, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr);
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail = pageInfoArray[numPages - 1];
    buffManager->pageFile = (char *) pageFileName;
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->totalNumOfFrames = numPages;
    buffManager->numPages = numPages;
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head = pageInfoArray[0];
    buffManager->strategy = strategy;
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes = 0;

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const buffManager) {
    RC valRes = checkBufManger(buffManager);
    return (forceFlushPool(buffManager) != RC_OK ? RC_FAILED_WRITEBACK : 0);
}

RC forceFlushPool(BM_BufferPool *const buffManager) {
    RC valRes = checkBufManger(buffManager);

    int temp = 0;
    pageInfo *pginformation;
    pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;

    loop:
    if (temp < ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->totalNumOfFrames) {
        if ((*pginformation).fixCount == 0) {
            if ((*pginformation).dirtyPage == 1) {
                writeBlock((*pginformation).pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                           (*pginformation).bufferData);
                (*pginformation).dirtyPage = 0;
                CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
                cache->writeCnt++;
                writeCnt++;
            }
        }
        pginformation = (*pginformation).nextPageInfo;
        temp++;
        goto loop;
    }

    return RC_OK;
}

RC markDirty(BM_BufferPool *const buffManager, BM_PageHandle *const page) {
    pageInfo *pginformation;
    if (buffManager == NULL)
        return RC_BUFFER_POOL_NOTFOUND;

    if (page == NULL)
        return RC_ERROR_PAGE;

    int temp = -1, counter = 0;
    pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    do {
        if (!((*pginformation).pageNum < (*page).pageNum) && !((*pginformation).pageNum > (*page).pageNum)) {
            break;
        } else if ((*pginformation).nextPageInfo != NULL) {
            pginformation = (*pginformation).nextPageInfo;
        } else {
            return 0;
        }
        counter++;
    } while (counter < (*buffManager).numPages);

    if (!(temp < (*buffManager).numPages) && !(temp > (*buffManager).numPages))
        return RC_READ_NON_EXISTING_PAGE;

    pginformation->dirtyPage = 1;
    return RC_OK;
}

RC unpinPage(BM_BufferPool *const buffManager, BM_PageHandle *const page) {
    pageInfo *pginformation;
    if (buffManager == NULL)
        return RC_BUFFER_POOL_NOTFOUND;

    if (page == NULL)
        return RC_ERROR_PAGE;

    pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    int temp = 0;

    while (!(temp >= (*buffManager).numPages)) {
        if (pginformation->pageNum == page->pageNum)
            break;
        pginformation = pginformation->nextPageInfo;
        temp++;
    }

    if (temp == (*buffManager).numPages) {
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        (*pginformation).fixCount = (*pginformation).fixCount - 1;
    }

    return RC_OK;
}

RC forcePage(BM_BufferPool *const buffManager, BM_PageHandle *const page) {
    if (buffManager == NULL)
        return RC_BUFFER_POOL_NOTFOUND;

    if (page == NULL)
        return RC_ERROR_PAGE;

    pageInfo *pginformation;
    pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    int temp = 0;
    bool flag = true;

    loop:
    if (!((*pginformation).pageNum<(*page).pageNum && (*pginformation).pageNum>(*page).pageNum))
        flag = false;
    if (flag) {
        pginformation = (*pginformation).nextPageInfo;
        temp++;
    }

    if (flag) {
        if (temp < (*buffManager).numPages) {
            goto loop;
        }
    }

    if (!flag && (temp < (*buffManager).numPages && temp > (*buffManager).numPages)) {
        return 1;
    }

    if (writeBlock(pginformation->pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                   pginformation->bufferData) != 0) {
        return RC_WRITE_FAILED;
    } else {

        CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
        cache->writeCnt++;
        writeCnt++;
    }

    return RC_OK;
}

RC pinPage(BM_BufferPool *const buffManager, BM_PageHandle *const page, const PageNumber pageNum) {
    int pno = 0;

    if (pno == 0) {
        if ((*buffManager).strategy == RS_FIFO) {
            pno = FIFOpin(buffManager, page, pageNum);
        } else {
            pno = LRUpin(buffManager, page, pageNum);
        }
    }

    return pno;
}

PageNumber *getFrameContents(BM_BufferPool *const buffManager) {
    PageNumber *array[buffManager->numPages];
    *array = malloc((*buffManager).numPages * sizeof(PageNumber));

    pageInfo *pginformation;
    int temp = 0;
    bool flag = true;
    condition1:
    pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    flag = true;
    condition2:
    if (pginformation->frameNum == temp) {
        (*array)[temp] = pginformation->pageNum;
        flag = false;
    }
    if (flag)
        pginformation = pginformation->nextPageInfo;
    if (pginformation != NULL && flag)
        goto condition2;
    temp++;
    if (temp < buffManager->numPages)
        goto condition1;
    return *array;
}

bool *getDirtyFlags(BM_BufferPool *const buffManager) {
    bool (*dirty)[buffManager->numPages];
    dirty = malloc(buffManager->numPages * sizeof(PageNumber));
    pageInfo *pginformation;
    int temp = 0;
    loop1:
    for (pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
         pginformation != NULL; pginformation = (*pginformation).nextPageInfo) {
        if (!((*pginformation).frameNum < temp || (*pginformation).frameNum > temp)) {
            if ((*pginformation).dirtyPage) {
                (*dirty)[temp] = TRUE;
            } else {
                (*dirty)[temp] = FALSE;
            }
            break;
        }
    }
    temp++;
    if (!(temp >= (*buffManager).numPages))
        goto loop1;

    return *dirty;
}


int *getFixCounts(BM_BufferPool *const buffManager) {
    pageInfo *pginformation;
    int temp = 0;
    int (*fxct)[(*buffManager).numPages];
    fxct = malloc((*buffManager).numPages * sizeof(PageNumber));

    loop:
    for (pginformation = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head; !(pginformation ==
                                                                                     NULL); pginformation = (*pginformation).nextPageInfo) {
        if (!((*pginformation).frameNum < temp || (*pginformation).frameNum > temp)) {
            (*fxct)[temp] = (*pginformation).fixCount;
            break;
        }
    }
    temp++;
    if (temp < (*buffManager).numPages) {
        goto loop;
    }
    return *fxct;
}

int getNumReadIO(BM_BufferPool *const buffManager) {
    CacheRequiredInfo *info = ((CacheRequiredInfo *) buffManager->mgmtData);
    return readsCnt;
}

int getNumWriteIO(BM_BufferPool *const buffManager) {
    int temp = writeCnt;
    return temp;
}
