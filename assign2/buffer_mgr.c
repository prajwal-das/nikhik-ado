#include <string.h>
#include <stdlib.h>
#include "dberror.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"


int readCnt, writeCnt;

typedef struct CacheRequiredInfo {
    SM_FileHandle *fileHandlerPntr;
    int writeCnt;
    char *bufferSize;
    int readCnt;
    Queue *queuePointer;
} CacheRequiredInfo;

RC checkBufManger(BM_BufferPool *bufferManager) {
    RC valRes = RC_OK;
    if (!bufferManager) {
        valRes = RC_BUFFER_POOL_NOTFOUND;
    }
    if (bufferManager->mgmtData == NULL) {
        valRes = RC_BUFFER_POOL_NOTFOUND;
    }
    return valRes;
}

CacheRequiredInfo *initCacheRequiredInfo(BM_BufferPool *const buffManager) {
    CacheRequiredInfo *cacheRequiredInfo = malloc(sizeof(CacheRequiredInfo));
    cacheRequiredInfo->readCnt = 0;
    readCnt = 0;
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

pageInfo *createNewPageInfo(int position, const PageNumber pageNum, const int fixCount) {
    pageInfo *pinf = calloc(PAGE_SIZE, sizeof(pageInfo));
    pinf->frameNum = position;
    pinf->dirtyPage = 0;
    pinf->bufferData = (char *) calloc(PAGE_SIZE, sizeof(char));
    pinf->pageNum = pageNum;
    pinf->fixCount = fixCount;
    return pinf;
}


RC pageFrameEnqueue(BM_PageHandle *const pageHandler, const PageNumber pageNum, BM_BufferPool *const buffManager) {
    RC valRes = checkBufManger(buffManager);
    pageInfo *curPageInfo = createNewPageInfo(0, pageNum, 1);
    (*curPageInfo).bufferData = (char *) malloc(PAGE_SIZE * sizeof(char));

    int dequeueCount = -1;

    if ((((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes ==
         ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->totalNumOfFrames)) {
        dequeueCount = deQueue(buffManager);
    }

    readBlock((*curPageInfo).pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
              (*curPageInfo).bufferData);
    CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
    cache->readCnt++;
    readCnt++;


    pageHandler->pageNum = pageNum;
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head->prevPageInfo = curPageInfo;
    pageHandler->data = curPageInfo->bufferData;
    curPageInfo->pageNum = pageNum;
    curPageInfo->nextPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    curPageInfo->frameNum = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head->frameNum;

    if (((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes == 0) {
        ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head = curPageInfo;
    } else {
        curPageInfo->frameNum = dequeueCount != -1 ? dequeueCount :
                                  ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head->frameNum + 1;
        ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head = curPageInfo;
    }

    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes++;

    return valRes;
}


pageInfo *getPageInfo(CacheRequiredInfo *cacheRequiredInfo, PageNumber n) {
    pageInfo *curPage = cacheRequiredInfo->queuePointer->head;
    while (curPage != NULL) {
        if (curPage->pageNum == n)
            return curPage;

        curPage = curPage->nextPageInfo;
    }
    return NULL;
}

RC pinStrategyPageLRU(BM_BufferPool *const buffManager, BM_PageHandle *const page, const PageNumber pageNum) {
    RC valRes = checkBufManger(buffManager);
    pageInfo *curPageInfo = getPageInfo(buffManager->mgmtData, pageNum);

    if (curPageInfo == NULL)
        pageFrameEnqueue(page, pageNum, buffManager);

    if (curPageInfo != NULL) {
        page->data = curPageInfo->bufferData;
        curPageInfo->fixCount++;
        page->pageNum = pageNum;

        if ((*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head == curPageInfo) {
            ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head = curPageInfo;
            curPageInfo->nextPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
            ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head->prevPageInfo = curPageInfo;
        } else {
            curPageInfo->prevPageInfo->nextPageInfo = curPageInfo->nextPageInfo;
        }

        curPageInfo->nextPageInfo->prevPageInfo = curPageInfo->prevPageInfo;

        if (curPageInfo != ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail) {
            curPageInfo->prevPageInfo = NULL;
            curPageInfo->nextPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
            curPageInfo->nextPageInfo->prevPageInfo = curPageInfo;
            ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head = curPageInfo;
        }

        if (curPageInfo == ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail) {
            ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail = curPageInfo->prevPageInfo;
            ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->nextPageInfo = NULL;
            curPageInfo->nextPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
            curPageInfo->nextPageInfo->prevPageInfo = curPageInfo;
            curPageInfo->prevPageInfo = NULL;
            (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head = curPageInfo;
        }

    }
    return valRes;
}

RC pinStrategyPageFIFO(BM_BufferPool *const buffManager, BM_PageHandle *const page, const PageNumber pageNum) {
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
            cache->readCnt++;
            readCnt++;
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
        writeBlock(page_info1->pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                   page_info1->bufferData);
    }

    (*addnode).frameNum = page_info1->frameNum;
    (*addnode).bufferData = page_info1->bufferData;
    readBlock(pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr, (*addnode).bufferData);
    (*page).data = (*addnode).bufferData;
    CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
    cache->readCnt++;
    readCnt++;
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->nextPageInfo = addnode;
    (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail = addnode;

    return RC_OK;
}

/**
 *
 */

RC initBufferPool(BM_BufferPool *const buffManager, const char *const pageFileName, const int numPages,
                  ReplacementStrategy strategy, void *stratData) {

    char *buffer_Size = (char *) calloc(numPages, sizeof(char) * PAGE_SIZE);

    CacheRequiredInfo *cacheRequiredInfo = initCacheRequiredInfo(buffManager);
    cacheRequiredInfo->queuePointer = (Queue *) malloc(sizeof(Queue));
    cacheRequiredInfo->bufferSize = buffer_Size;
    (*buffManager).mgmtData = cacheRequiredInfo;


    pageInfo *pageInfoArray[numPages];

    for (int curPage = 0; curPage < numPages; curPage++) {
        pageInfoArray[curPage] = createNewPageInfo(curPage, -1, 0);
    }

    for (int curPage = 0; curPage < numPages; curPage++) {

        pageInfoArray[curPage]->nextPageInfo = pageInfoArray[curPage + 1];
        pageInfoArray[curPage]->prevPageInfo = pageInfoArray[curPage - 1];

        if (curPage == 0) {
            pageInfoArray[curPage]->prevPageInfo = NULL;
        } else if (curPage == numPages - 1) {
            pageInfoArray[curPage]->nextPageInfo = NULL;
        }
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
    if (valRes != RC_OK) {
        return valRes;
    }

    pageInfo *curHead = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    while (NULL != curHead) {
        if (curHead->dirtyPage != 1) {
            curHead = curHead->nextPageInfo;
            continue;
        }

        valRes = writeBlock((curHead)->pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                            (curHead)->bufferData);
        curHead->dirtyPage = 0;
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
        cache->writeCnt++;
        writeCnt++;
        curHead = curHead->nextPageInfo;
    }

    return valRes;
}

RC markDirty(BM_BufferPool *const buffManager, BM_PageHandle *const page) {
    RC valRes = checkBufManger(buffManager);
    pageInfo *curNode = getPageInfo(buffManager->mgmtData, page->pageNum);
    if (NULL == curNode)
        return RC_READ_NON_EXISTING_PAGE;

    curNode->dirtyPage = 1;
    return valRes;
}

RC unpinPage(BM_BufferPool *const buffManager, BM_PageHandle *const page) {
    RC valRes = checkBufManger(buffManager);

    pageInfo *curNode = getPageInfo(buffManager->mgmtData, page->pageNum);
    if (curNode == NULL)
        return RC_READ_NON_EXISTING_PAGE;

    curNode->fixCount--;
    return valRes;
}

RC forcePage(BM_BufferPool *const buffManager, BM_PageHandle *const page) {
    RC valRes = checkBufManger(buffManager);
    pageInfo *curNode = getPageInfo(buffManager->mgmtData, page->pageNum);
    if (curNode == NULL)
        return RC_READ_NON_EXISTING_PAGE;

    valRes = writeBlock(curNode->pageNum,
                        ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                        curNode->bufferData);

    if (valRes != RC_OK)
        return RC_WRITE_FAILED;

    CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
    cache->writeCnt++;
    writeCnt++;

    return valRes;
}

RC pinPage(BM_BufferPool *const buffManager, BM_PageHandle *const page, const PageNumber pageNum) {
    RC valRes = checkBufManger(buffManager);
    switch (buffManager->strategy) {
        case RS_FIFO:
            valRes = pinStrategyPageFIFO(buffManager, page, pageNum);
            break;
        default:
            valRes = pinStrategyPageLRU(buffManager, page, pageNum);
    }
    return valRes;
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
    RC valRes = checkBufManger(buffManager);
    bool (*dirFlags)[buffManager->numPages] = calloc(PAGE_SIZE, buffManager->numPages * sizeof(PageNumber));
    pageInfo *pagInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;

    while (pagInfo != NULL) {
        pageInfo *curPage = pagInfo;
        pagInfo = pagInfo->nextPageInfo;
        (*dirFlags)[curPage->frameNum] = curPage->dirtyPage ? TRUE : FALSE;
    }

    return *dirFlags;
}


int *getFixCounts(BM_BufferPool *const buffManager) {
    RC valRes = checkBufManger(buffManager);
    int (*fxCnt)[buffManager->numPages] = calloc(PAGE_SIZE, buffManager->numPages * sizeof(PageNumber));
    pageInfo *pagInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;

    while (pagInfo != NULL) {
        pageInfo *curPage = pagInfo;
        pagInfo = pagInfo->nextPageInfo;
        (*fxCnt)[curPage->frameNum] = curPage->fixCount;
    }

    return *fxCnt;
}

int getNumReadIO(BM_BufferPool *const buffManager) {
    CacheRequiredInfo *info = ((CacheRequiredInfo *) buffManager->mgmtData);
    return readCnt;
}

int getNumWriteIO(BM_BufferPool *const buffManager) {
    CacheRequiredInfo *info = ((CacheRequiredInfo *) buffManager->mgmtData);
    return writeCnt;
}
