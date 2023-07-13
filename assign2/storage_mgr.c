#include <stdio.h>
#include <stdlib.h>
#include "storage_mgr.h"
#include "dberror.h"

static const size_t BIT_SIZE = sizeof(char);

void initStorageManager(void) {
    printf("The Storage Manager is being initialized...\n");
}


int numberOfPages(int size, FILE *storageFile) {
    // Set the file position indicator to 0
    fseek(storageFile, 0, SEEK_END);
    // get the current file position
    long records = ftell(storageFile);
    // reset to starting position
    rewind(storageFile);
    // number of page can be evaluated by dividing file end position with size of page
    return records / size;
}

RC checkFile(char *fileName) {
    FILE *storageFile = fopen(fileName, "r");
    if (!storageFile) {
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

/* Creating a Page File */
RC createPageFile(char *fileName) {
    FILE *storageFile = fopen(fileName, "w");

    if (!storageFile) {
        return RC_FILE_NOT_FOUND;
    }

    char *memBlk = calloc(PAGE_SIZE, BIT_SIZE);
    fwrite(memBlk, BIT_SIZE, PAGE_SIZE, storageFile);
    fclose(storageFile);
    free(memBlk);
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fh) {
    RC validResponse = checkFile(fileName);
    if (validResponse != RC_OK) {
        return validResponse;
    }

    FILE *storageFile = fopen(fileName, "r");
    fh->totalNumPages = numberOfPages(PAGE_SIZE, storageFile);
    fh->mgmtInfo = storageFile;
    fh->curPagePos = 0;
    fh->fileName = fileName;
    return validResponse;
}

RC closePageFile(SM_FileHandle *fh) {
    RC validResponse = checkFile(fh->fileName);
    fclose(fh->mgmtInfo);
    return validResponse;
}

RC destroyPageFile(char *fileName) {
    return remove(fileName) != 0 ? RC_FILE_NOT_FOUND : RC_OK;
}

RC readBlock(int pageNum, SM_FileHandle *fh, SM_PageHandle memPage) {
    RC validResponse = checkFile(fh->fileName);

    if (pageNum < 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    FILE *storageFile = fopen(fh->fileName, "r+");
    // Set the file position indicator to a starting of `pageNum`
    fseek(storageFile, pageNum * PAGE_SIZE, SEEK_SET);
    fread(memPage, BIT_SIZE, PAGE_SIZE, storageFile);
    fh->curPagePos = pageNum;
    return validResponse;
}

RC getBlockPos(SM_FileHandle *fh) {
    RC validResponse = checkFile(fh->fileName);
    return validResponse != RC_OK ? validResponse : fh->curPagePos;
}


RC readFirstBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC validResponse = checkFile(fh->fileName);
    int pageNum = 0;
    return validResponse != RC_OK ? validResponse : readBlock(pageNum, fh, memPage);
}


RC readPreviousBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC validResponse = checkFile(fh->fileName);
    int pageNum = getBlockPos(fh) - 1;
    return validResponse != RC_OK ? validResponse : readBlock(pageNum, fh, memPage);
}


RC readCurrentBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC validResponse = checkFile(fh->fileName);
    int pageNum = getBlockPos(fh);
    return validResponse != RC_OK ? validResponse : readBlock(pageNum, fh, memPage);
}


RC readNextBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC validResponse = checkFile(fh->fileName);
    int pageNum = fh->curPagePos + 1;
    return validResponse != RC_OK ? validResponse : readBlock(pageNum, fh, memPage);
}

RC readLastBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC validResponse = checkFile(fh->fileName);
    return validResponse != RC_OK ? validResponse : readBlock(
            numberOfPages(BIT_SIZE,
                          fh->mgmtInfo) - 1, fh, memPage);
}


RC writeBlock(int pageNum, SM_FileHandle *fh, SM_PageHandle memPage) {

    RC validResponse = checkFile(fh->fileName);
    if (validResponse != RC_OK) {
        return validResponse;
    }
    if (pageNum < 0 || pageNum > fh->totalNumPages) {
        return RC_WRITE_FAILED;
    }

    FILE *storageFile = fopen(fh->fileName, "r+");
    fwrite(memPage, BIT_SIZE, PAGE_SIZE, storageFile);
    // Set the file position indicator to 0
    fseek(storageFile, 0, SEEK_END);
    fh->totalNumPages = numberOfPages(BIT_SIZE, storageFile);
    fh->curPagePos = pageNum;
    return validResponse;
}

RC writeCurrentBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC validResponse = checkFile(fh->fileName);
    return validResponse != RC_OK ? validResponse : writeBlock(fh->curPagePos, fh, memPage);
}


RC appendEmptyBlock(SM_FileHandle *fh) {
    RC validResponse = checkFile(fh->fileName);
    if (validResponse != RC_OK) {
        return validResponse;
    }
    char *memBlk = calloc(PAGE_SIZE, BIT_SIZE);

    FILE *storageFile = fopen(fh->fileName, "r+");
    fwrite(memBlk, 1, PAGE_SIZE, storageFile);
    fh->curPagePos = numberOfPages(BIT_SIZE, storageFile) - 1;
    fh->totalNumPages = numberOfPages(BIT_SIZE, storageFile);
    free(memBlk);

    return validResponse;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fh) {
    RC validResponse = checkFile(fh->fileName);
    if (validResponse != RC_OK) {
        return validResponse;
    }

    if (fh->totalNumPages > numberOfPages) {
        return RC_OK;
    }

    while (fh->totalNumPages < numberOfPages) {
        appendEmptyBlock(fh);
    }
    return validResponse;
}