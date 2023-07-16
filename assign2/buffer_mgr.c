#include <string.h>
#include <stdlib.h>
#include "dberror.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"


int readsCount, writesCount;

typedef struct CacheRequiredInfo {
    SM_FileHandle *fh;
    int writesCount;
    int readsCount;
    char *bufferSize;
    Queue *q;
} CacheRequiredInfo;


CacheRequiredInfo *initCacheRequiredInfo() {
    CacheRequiredInfo *cacheRequiredInfo = malloc(sizeof(CacheRequiredInfo));
    cacheRequiredInfo->readsCount = 0;
    cacheRequiredInfo->writesCount = 0;
    cacheRequiredInfo->fh = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
}

RC deQueue(BM_BufferPool *const bm) {
    int counter = 0;
    pageInfo *pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head;

    loop:
    if (counter == (((CacheRequiredInfo *) bm->mgmtData)->q->filledframes - 1))
        (*((CacheRequiredInfo *) bm->mgmtData)->q).tail = pginformation;
    else
        pginformation = (*pginformation).nextPageInfo;

    counter++;

    if (counter < (*((CacheRequiredInfo *) bm->mgmtData)->q).filledframes)
        goto loop;

    // Setting temp variables
    int tailnum;
    int pageDelete = 0;
    pageInfo *pinfo = ((CacheRequiredInfo *) bm->mgmtData)->q->tail;
    counter = 0;

    while (counter < ((CacheRequiredInfo *) bm->mgmtData)->q->totalNumOfFrames) {
        counter++;
        if ((pinfo->fixCount) == 0) {
            pageDelete = (*pinfo).pageNum;
            if ((*pinfo).pageNum != ((CacheRequiredInfo *) bm->mgmtData)->q->tail->pageNum) {
                (*pinfo).nextPageInfo->prevPageInfo = (*pinfo).prevPageInfo;
                (*pinfo).prevPageInfo->nextPageInfo = (*pinfo).nextPageInfo;
            } else {
                (*((CacheRequiredInfo *) bm->mgmtData)->q).tail = (*((CacheRequiredInfo *) bm->mgmtData)->q).tail->prevPageInfo;
                (*((CacheRequiredInfo *) bm->mgmtData)->q).tail->nextPageInfo = NULL;
            }
        } else {
            tailnum = (*pinfo).pageNum;
            pinfo = (*pinfo).prevPageInfo;
        }
    }

    if ((*pinfo).dirtyPage == 1) {
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) bm->mgmtData);
        cache->writesCount++;
        writesCount++;
        writeBlock(pinfo->pageNum, ((CacheRequiredInfo *) bm->mgmtData)->fh, pinfo->bufferData);
    }
    ((CacheRequiredInfo *) bm->mgmtData)->q->filledframes--;

    if (tailnum == ((CacheRequiredInfo *) bm->mgmtData)->q->tail->pageNum)
        return 0;

    return pageDelete;
}

RC Enq_pageFragme(BM_PageHandle *const page, const PageNumber pageNum, BM_BufferPool *const bm) {
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

    if (!((*((CacheRequiredInfo *) bm->mgmtData)->q).filledframes != (*((CacheRequiredInfo *) bm->mgmtData)->q).totalNumOfFrames)) { // check if frames are full. If full remove pages
        pgDel_counter = deQueue(bm);
    }

    if ((*((CacheRequiredInfo *) bm->mgmtData)->q).filledframes != 0) {
        readBlock(pageNum, ((CacheRequiredInfo *) bm->mgmtData)->fh, pginformation->bufferData);    // read block of data
        if (pgDel_counter == -1)
            pginformation->frameNum = ((CacheRequiredInfo *) bm->mgmtData)->q->head->frameNum + 1;
        else
            pginformation->frameNum = pgDel_counter;
        page->data = pginformation->bufferData;
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) bm->mgmtData);
        cache->readsCount++;
        readsCount++;
        pginformation->nextPageInfo = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
        ((CacheRequiredInfo *) bm->mgmtData)->q->head->prevPageInfo = pginformation;
        ((CacheRequiredInfo *) bm->mgmtData)->q->head = pginformation;
        page->pageNum = pageNum;
    } else {
        readBlock((*pginformation).pageNum, ((CacheRequiredInfo *) bm->mgmtData)->fh, (*pginformation).bufferData);
        (*page).data = (*pginformation).bufferData;
        (*pginformation).pageNum = pageNum;
        (*page).pageNum = pageNum;
        (*pginformation).nextPageInfo = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
        ((CacheRequiredInfo *) bm->mgmtData)->q->head->prevPageInfo = pginformation;
        (*pginformation).frameNum = ((CacheRequiredInfo *) bm->mgmtData)->q->head->frameNum;
        ((CacheRequiredInfo *) bm->mgmtData)->q->head = pginformation;
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) bm->mgmtData);
        cache->readsCount++;
        readsCount++;
    }
    (*((CacheRequiredInfo *) bm->mgmtData)->q).filledframes++;

    return RC_OK;
}

RC LRUpin(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    pageInfo *pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
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
    if (temp < bm->numPages) {
        goto loop;
    }

    if (pageFragment == 1) {
        (*page).pageNum = pageNum;
        (*page).data = (*pginformation).bufferData;
        (*pginformation).fixCount++;

        if (pginformation != (*((CacheRequiredInfo *) bm->mgmtData)->q).head) {
            (*pginformation).prevPageInfo->nextPageInfo = (*pginformation).nextPageInfo;
        } else {
            (*((CacheRequiredInfo *) bm->mgmtData)->q).head = pginformation;
            (*pginformation).nextPageInfo = (*((CacheRequiredInfo *) bm->mgmtData)->q).head;
            (*((CacheRequiredInfo *) bm->mgmtData)->q).head->prevPageInfo = pginformation;
        }

        if ((*pginformation).nextPageInfo && pginformation == (*((CacheRequiredInfo *) bm->mgmtData)->q).head) {
            return 0;
        } else if (pginformation != ((CacheRequiredInfo *) bm->mgmtData)->q->head && (*pginformation).nextPageInfo) {
            (*pginformation).nextPageInfo->prevPageInfo = (*pginformation).prevPageInfo;
            if (pginformation == (*((CacheRequiredInfo *) bm->mgmtData)->q).tail) {
                (*((CacheRequiredInfo *) bm->mgmtData)->q).tail = (*pginformation).prevPageInfo;
                ((CacheRequiredInfo *) bm->mgmtData)->q->tail->nextPageInfo = NULL;
                (*pginformation).prevPageInfo = NULL;
                (*pginformation).nextPageInfo = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
                (*pginformation).nextPageInfo->prevPageInfo = pginformation;
                (*((CacheRequiredInfo *) bm->mgmtData)->q).head = pginformation;
            } else {
                (*pginformation).prevPageInfo = NULL;
                (*pginformation).nextPageInfo = (*((CacheRequiredInfo *) bm->mgmtData)->q).head;
                (*pginformation).nextPageInfo->prevPageInfo = pginformation;
                (*((CacheRequiredInfo *) bm->mgmtData)->q).head = pginformation;
            }
        }

    } else if (pageFragment == 0)
        Enq_pageFragme(page, pageNum, bm);
    return RC_OK;
}

RC FIFOpin(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    int numPages = bm->numPages;
    int pageFound = 0;

    pageInfo *pgList = NULL;
    pgList = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
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

    pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
    int returncode = -1;

    do {
        if (pginformation->pageNum == -1) {
            (*pginformation).fixCount = 1;
            (*pginformation).dirtyPage = 0;
            (*pginformation).pageNum = pageNum;
            (*page).pageNum = pageNum;

            (*((CacheRequiredInfo *) bm->mgmtData)->q).filledframes = (*((CacheRequiredInfo *) bm->mgmtData)->q).filledframes + 1;

            readBlock((*pginformation).pageNum, ((CacheRequiredInfo *) bm->mgmtData)->fh, (*pginformation).bufferData);

            (*page).data = (*pginformation).bufferData;
            CacheRequiredInfo *cache = ((CacheRequiredInfo *) bm->mgmtData);
            cache->readsCount++;
            readsCount++;
            returncode = 0;
            break;
        } else {
            pginformation = pginformation->nextPageInfo;
        }

    } while (((CacheRequiredInfo *) bm->mgmtData)->q->filledframes < ((CacheRequiredInfo *) bm->mgmtData)->q->totalNumOfFrames);

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
    addnode->prevPageInfo = (*((CacheRequiredInfo *) bm->mgmtData)->q).tail;
    pginformation = (*((CacheRequiredInfo *) bm->mgmtData)->q).head;

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


    if (pginformation != ((CacheRequiredInfo *) bm->mgmtData)->q->head && pginformation != ((CacheRequiredInfo *) bm->mgmtData)->q->tail) {
        (*pginformation).nextPageInfo->prevPageInfo = (*pginformation).prevPageInfo;
        (*pginformation).prevPageInfo->nextPageInfo = (*pginformation).nextPageInfo;
    }

    if (temp == numPages) {
        return RC_NO_FREESPACE_ERROR;
    }

    switch (pginformation != ((CacheRequiredInfo *) bm->mgmtData)->q->head && pginformation != ((CacheRequiredInfo *) bm->mgmtData)->q->tail) {
        case true:
            pginformation->prevPageInfo->nextPageInfo = pginformation->nextPageInfo;
            pginformation->nextPageInfo->prevPageInfo = pginformation->prevPageInfo;
            break;
    }

    if ((*((CacheRequiredInfo *) bm->mgmtData)->q).head == pginformation) {
        (*((CacheRequiredInfo *) bm->mgmtData)->q).head = (*((CacheRequiredInfo *) bm->mgmtData)->q).head->nextPageInfo;
        (*((CacheRequiredInfo *) bm->mgmtData)->q).head->prevPageInfo = NULL;
    }

    switch (pginformation == ((CacheRequiredInfo *) bm->mgmtData)->q->tail) {
        case true:
            ((CacheRequiredInfo *) bm->mgmtData)->q->tail = pginformation->prevPageInfo;
            addnode->prevPageInfo = ((CacheRequiredInfo *) bm->mgmtData)->q->tail;
            break;
    }

    // If page is dirty, we write back to the disk

    if ((*page_info1).dirtyPage == 1) {
        CacheRequiredInfo *cache = ((CacheRequiredInfo *) bm->mgmtData);
        cache->writesCount++;
        writesCount++;
        writeBlock(page_info1->pageNum, ((CacheRequiredInfo *) bm->mgmtData)->fh, page_info1->bufferData);
    }

    (*addnode).frameNum = page_info1->frameNum;
    (*addnode).bufferData = page_info1->bufferData;
    readBlock(pageNum, ((CacheRequiredInfo *) bm->mgmtData)->fh, (*addnode).bufferData);
    (*page).data = (*addnode).bufferData;
    CacheRequiredInfo *cache = ((CacheRequiredInfo *) bm->mgmtData);
    cache->readsCount++;
    readsCount++;
    ((CacheRequiredInfo *) bm->mgmtData)->q->tail->nextPageInfo = addnode;
    (*((CacheRequiredInfo *) bm->mgmtData)->q).tail = addnode;

    return RC_OK;
}


//  --- main


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages,
                  ReplacementStrategy strategy, void *stratData) {

    char *buffer_Size = (char *) calloc(numPages, sizeof(char) * PAGE_SIZE);

    CacheRequiredInfo *cacheRequiredInfo = initCacheRequiredInfo();
    cacheRequiredInfo->q = (Queue *) malloc(sizeof(Queue));
    cacheRequiredInfo->bufferSize = buffer_Size;

    readsCount = 0;
    (*bm).pageFile = (char *) pageFileName;
    (*bm).numPages = numPages;
    (*bm).strategy = strategy;
    (*bm).mgmtData = cacheRequiredInfo;
    writesCount = 0;

    pageInfo *pginformation[(*bm).numPages];
    int lp = ((*bm).numPages) - 1;
    int temp = 0;
    loop:
    pginformation[temp] = (pageInfo *) malloc(sizeof(pageInfo));
    temp++;
    if (!(temp > lp)) {
        goto loop;
    }
    temp = 0;
    loop1:
    pginformation[temp]->fixCount = 0;
    pginformation[temp]->pageNum = -1;
    pginformation[temp]->frameNum = temp;
    pginformation[temp]->dirtyPage = 0;
    pginformation[temp]->bufferData = (char *) calloc(PAGE_SIZE, sizeof(char));
    temp++;
    if (temp <= lp) {
        goto loop1;
    }
    int counter = 0;
    loop2:
    temp = counter;
    if (temp == lp && temp == 0) {
        pginformation[temp]->prevPageInfo = (temp == 0) ? NULL : pginformation[temp - 1];
        pginformation[temp]->nextPageInfo = (temp == 0) ? pginformation[temp + 1] : NULL;
    } else {
        pginformation[temp]->nextPageInfo = pginformation[temp + 1];
        pginformation[temp]->prevPageInfo = pginformation[temp - 1];
    }
    counter++;
    if (counter <= lp)
        goto loop2;
    ((CacheRequiredInfo *) bm->mgmtData)->q->filledframes = 0;
    ((CacheRequiredInfo *) bm->mgmtData)->q->totalNumOfFrames = (*bm).numPages;
    ((CacheRequiredInfo *) bm->mgmtData)->q->head = pginformation[0];
    ((CacheRequiredInfo *) bm->mgmtData)->q->tail = pginformation[lp];

    openPageFile(bm->pageFile, ((CacheRequiredInfo *) bm->mgmtData)->fh);

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {
    if (bm == NULL)
        return RC_BUFFER_POOL_NOTFOUND;

    int res = forceFlushPool(bm);
    return (res != RC_OK ? RC_FAILED_WRITEBACK : 0);
}

RC forceFlushPool(BM_BufferPool *const bm) {
    if (bm == NULL)
        return RC_BUFFER_POOL_NOTFOUND;
    int temp = 0;
    pageInfo *pginformation;
    pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head;

    loop:
    if (temp < ((CacheRequiredInfo *) bm->mgmtData)->q->totalNumOfFrames) {
        if ((*pginformation).fixCount == 0) {
            if ((*pginformation).dirtyPage == 1) {
                writeBlock((*pginformation).pageNum, ((CacheRequiredInfo *) bm->mgmtData)->fh, (*pginformation).bufferData);
                (*pginformation).dirtyPage = 0;
                CacheRequiredInfo *cache = ((CacheRequiredInfo *) bm->mgmtData);
                cache->writesCount++;
                writesCount++;
            }
        }
        pginformation = (*pginformation).nextPageInfo;
        temp++;
        goto loop;
    }

    return RC_OK;
}

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    pageInfo *pginformation;
    if (bm == NULL)
        return RC_BUFFER_POOL_NOTFOUND;

    if (page == NULL)
        return RC_ERROR_PAGE;

    int temp = -1, counter = 0;
    pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
    do {
        if (!((*pginformation).pageNum < (*page).pageNum) && !((*pginformation).pageNum > (*page).pageNum)) {
            break;
        } else if ((*pginformation).nextPageInfo != NULL) {
            pginformation = (*pginformation).nextPageInfo;
        } else {
            return 0;
        }
        counter++;
    } while (counter < (*bm).numPages);

    if (!(temp < (*bm).numPages) && !(temp > (*bm).numPages))
        return RC_READ_NON_EXISTING_PAGE;

    pginformation->dirtyPage = 1;
    return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    pageInfo *pginformation;
    if (bm == NULL)
        return RC_BUFFER_POOL_NOTFOUND;

    if (page == NULL)
        return RC_ERROR_PAGE;

    pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
    int temp = 0;

    while (!(temp >= (*bm).numPages)) {
        if (pginformation->pageNum == page->pageNum)
            break;
        pginformation = pginformation->nextPageInfo;
        temp++;
    }

    if (temp == (*bm).numPages) {
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        (*pginformation).fixCount = (*pginformation).fixCount - 1;
    }

    return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm == NULL)
        return RC_BUFFER_POOL_NOTFOUND;

    if (page == NULL)
        return RC_ERROR_PAGE;

    pageInfo *pginformation;
    pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
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
        if (temp < (*bm).numPages) {
            goto loop;
        }
    }

    if (!flag && (temp < (*bm).numPages && temp > (*bm).numPages)) {
        return 1;
    }

    if (writeBlock(pginformation->pageNum, ((CacheRequiredInfo *) bm->mgmtData)->fh, pginformation->bufferData) != 0) {
        return RC_WRITE_FAILED;
    } else {

        CacheRequiredInfo *cache = ((CacheRequiredInfo *) bm->mgmtData);
        cache->writesCount++;
        writesCount++;
    }

    return RC_OK;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    int pno = 0;

    if (pno == 0) {
        if ((*bm).strategy == RS_FIFO) {
            pno = FIFOpin(bm, page, pageNum);
        } else {
            pno = LRUpin(bm, page, pageNum);
        }
    }

    return pno;
}

PageNumber *getFrameContents(BM_BufferPool *const bm) {
    PageNumber *array[bm->numPages];
    *array = malloc((*bm).numPages * sizeof(PageNumber));

    pageInfo *pginformation;
    int temp = 0;
    bool flag = true;
    condition1:
    pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head;
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
    if (temp < bm->numPages)
        goto condition1;
    return *array;
}

bool *getDirtyFlags(BM_BufferPool *const bm) {
    bool (*dirty)[bm->numPages];
    dirty = malloc(bm->numPages * sizeof(PageNumber));
    pageInfo *pginformation;
    int temp = 0;
    loop1:
    for (pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head; pginformation != NULL; pginformation = (*pginformation).nextPageInfo) {
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
    if (!(temp >= (*bm).numPages))
        goto loop1;

    return *dirty;
}


int *getFixCounts(BM_BufferPool *const bm) {
    pageInfo *pginformation;
    int temp = 0;
    int (*fxct)[(*bm).numPages];
    fxct = malloc((*bm).numPages * sizeof(PageNumber));

    loop:
    for (pginformation = ((CacheRequiredInfo *) bm->mgmtData)->q->head; !(pginformation == NULL); pginformation = (*pginformation).nextPageInfo) {
        if (!((*pginformation).frameNum < temp || (*pginformation).frameNum > temp)) {
            (*fxct)[temp] = (*pginformation).fixCount;
            break;
        }
    }
    temp++;
    if (temp < (*bm).numPages) {
        goto loop;
    }
    return *fxct;
}

int getNumReadIO(BM_BufferPool *const bm) {
    CacheRequiredInfo *info = ((CacheRequiredInfo *) bm->mgmtData);
    return readsCount;
}

int getNumWriteIO(BM_BufferPool *const bm) {
    int temp = writesCount;
    return temp;
}
