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

void resetTailToFrameEnd(BM_BufferPool *const buffManager) {
    pageInfo *pageHead = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;

    for (int position = 0;
         position != (((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes - 1); position++) {
        pageHead = pageHead->nextPageInfo;
    }
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail = pageHead;
}

CacheRequiredInfo *initCacheRequiredInfo(BM_BufferPool *const buffManager) {
    CacheRequiredInfo *cacheRequiredInfo = malloc(sizeof(CacheRequiredInfo));
    cacheRequiredInfo->readCnt = 0;
    readCnt = 0;
    cacheRequiredInfo->fileHandlerPntr = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
    cacheRequiredInfo->writeCnt = 0;
    writeCnt = 0;
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

RC pageFrameDequeue(BM_BufferPool *const buffManager) {

    resetTailToFrameEnd(buffManager);

    int removed = 0;

    int iterate = 0;
    while (((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->totalNumOfFrames >= iterate) {
        iterate++;
        if (0 == ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->fixCount) {
            removed = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->pageNum;
        }
    }

    if (((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->dirtyPage == 1) {
        writeBlock(((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->pageNum,
                   ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                   ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->bufferData);
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
        cache->writeCnt++;
        writeCnt++;
    }
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes--;

    return removed;
}

RC pageFrameEnqueue(BM_PageHandle *const pageHandler, const PageNumber pageNum, BM_BufferPool *const buffManager) {
    RC valRes = checkBufManger(buffManager);
    pageInfo *curPageInfo = createNewPageInfo(0, pageNum, 1);
    (*curPageInfo).bufferData = (char *) malloc(PAGE_SIZE * sizeof(char));

    int dequeueCount = -1;

    if ((((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->filledframes ==
         ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->totalNumOfFrames)) {
        dequeueCount = pageFrameDequeue(buffManager);
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
    RC valRes = checkBufManger(buffManager);
    pageInfo *pgList = getPageInfo(buffManager->mgmtData, pageNum);

    if (pgList != NULL) {
        page->data = pgList->bufferData;
        page->pageNum = pageNum;
        pgList->fixCount++;
        return valRes;
    }

    pageInfo *nPageInfo = createNewPageInfo(0, pageNum, 1);
    page->pageNum = pageNum;
    nPageInfo->prevPageInfo = (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).tail;
    pageInfo *curPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;

    for (int pos = 0; pos < buffManager->numPages &&
                      curPageInfo->fixCount != 0; pos++, curPageInfo = (*curPageInfo).nextPageInfo) {
        if (pos == buffManager->numPages) {
            return RC_NO_FREESPACE_ERROR;
        }
    }


    pageInfo *curInfo = curPageInfo;
    if ((*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head == curPageInfo) {
        (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head->prevPageInfo = NULL;
        (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head =
                (*((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer).head->nextPageInfo;
    } else if (curPageInfo == ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail) {
        ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail = curPageInfo->prevPageInfo;
        nPageInfo->prevPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail;
    } else {
        curPageInfo->nextPageInfo->prevPageInfo = curPageInfo->prevPageInfo;
        curPageInfo->prevPageInfo->nextPageInfo = curPageInfo->nextPageInfo;
    }

    if (curInfo->dirtyPage == 1) {
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
        writeBlock(curInfo->pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr,
                   curInfo->bufferData);
        cache->writeCnt++;
        writeCnt++;
    }

    nPageInfo->frameNum = curInfo->frameNum;
    nPageInfo->bufferData = curInfo->bufferData;
    readBlock(pageNum, ((CacheRequiredInfo *) buffManager->mgmtData)->fileHandlerPntr, (*nPageInfo).bufferData);
    page->data = nPageInfo->bufferData;
    CacheRequiredInfo *cache = ((CacheRequiredInfo *) buffManager->mgmtData);
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail->nextPageInfo = nPageInfo;
    cache->readCnt++;
    ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->tail = nPageInfo;
    readCnt++;

    return RC_OK;
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

    pageInfo *curPageInfo;
    int temp = 0;
    bool flag = true;
    condition1:
    curPageInfo = ((CacheRequiredInfo *) buffManager->mgmtData)->queuePointer->head;
    flag = true;
    condition2:
    if (curPageInfo->frameNum == temp) {
        (*array)[temp] = curPageInfo->pageNum;
        flag = false;
    }
    if (flag)
        curPageInfo = curPageInfo->nextPageInfo;
    if (curPageInfo != NULL && flag)
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
