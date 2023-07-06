#include <stdio.h>
#include <stdlib.h>
#include "storage_mgr.h"
#include "dberror.h"

static const size_t SIZE = sizeof(char);

/*
 * This function starts the storage manager. It prints a message indicating that the storage manager is being initialized.
 */
void initStorageManager(void) {
    printf("The Storage Manager is being initialized...\n");
}

int numberOfPages(int size, FILE *openFile) {
    fseek(openFile, 0, SEEK_END);
    long records = ftell(openFile);
    rewind(openFile);
    return records / size;
}

RC checkFile(char *fileName) {
    FILE *opFile = fopen(fileName, "r");
    if (!opFile) {
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

/* Creating a Page File */
RC createPageFile(char *fileName) {
    FILE *opFile = fopen(fileName, "w");

    if (!opFile) {
        return RC_FILE_NOT_FOUND;
    }

    char *memBlk = calloc(PAGE_SIZE, SIZE);
    fwrite(memBlk, SIZE, PAGE_SIZE, opFile);
    fclose(opFile);
    free(memBlk);
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    RC response = checkFile(fileName);
    if (response != RC_OK) {
        return response;
    }

    FILE *opFile = fopen(fileName, "r");
    fHandle->totalNumPages = numberOfPages(PAGE_SIZE, opFile);
    fHandle->mgmtInfo = opFile;
    fHandle->curPagePos = 0;
    fHandle->fileName = fileName;
    return response;
}

RC closePageFile(SM_FileHandle *fHandle) {
    RC response = checkFile(fHandle->fileName);
    fclose(fHandle->mgmtInfo);
    return response;
}

RC destroyPageFile(char *fileName) {
    return remove(fileName) != 0 ? RC_FILE_NOT_FOUND : RC_OK;
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC response = checkFile(fHandle->fileName);

    if (pageNum < 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    FILE *opFile = fopen(fHandle->fileName, "r+");
    fseek(opFile, pageNum * PAGE_SIZE, SEEK_SET);
    fread(memPage, SIZE, PAGE_SIZE, opFile);
    fHandle->curPagePos = pageNum;
    return response;
}

RC getBlockPos(SM_FileHandle *fh) {
    RC response = checkFile(fh->fileName);
    return response != RC_OK ? response : fh->curPagePos;
}


RC readFirstBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC response = checkFile(fh->fileName);
    int pageNum = 0;
    return response != RC_OK ? response : readBlock(pageNum, fh, memPage);
}


RC readPreviousBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC response = checkFile(fh->fileName);
    int pageNum = getBlockPos(fh) - 1;
    return response != RC_OK ? response : readBlock(pageNum, fh, memPage);
}


RC readCurrentBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC response = checkFile(fh->fileName);
    int pageNum = getBlockPos(fh);
    return response != RC_OK ? response : readBlock(pageNum, fh, memPage);
}


RC readNextBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC response = checkFile(fh->fileName);
    int pageNum = fh->curPagePos + 1;
    return response != RC_OK ? response : readBlock(pageNum, fh, memPage);
}

RC readLastBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC response = checkFile(fh->fileName);
    return response != RC_OK ? response : readBlock(
            numberOfPages(SIZE,
                          fh->mgmtInfo) - 1, fh, memPage);
}


RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    RC response = checkFile(fHandle->fileName);
    if (response != RC_OK) {
        return response;
    }
    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_WRITE_FAILED;
    }

    FILE *opFile = fopen(fHandle->fileName, "r+");
    fwrite(memPage, SIZE, PAGE_SIZE, opFile);
    fseek(opFile, 0, SEEK_END);
    fHandle->totalNumPages = numberOfPages(SIZE, opFile);
    fHandle->curPagePos = pageNum;
    return response;
}

RC writeCurrentBlock(SM_FileHandle *fh, SM_PageHandle memPage) {
    RC response = checkFile(fh->fileName);
    return response != RC_OK ? response : writeBlock(fh->curPagePos, fh, memPage);
}


RC appendEmptyBlock(SM_FileHandle *fHandle) {
    RC response = checkFile(fHandle->fileName);
    if (response != RC_OK) {
        return response;
    }
    char *memBlk = calloc(PAGE_SIZE, SIZE);

    FILE *opFile = fopen(fHandle->fileName, "r+");
    fwrite(memBlk, 1, PAGE_SIZE, opFile);
    fHandle->curPagePos = numberOfPages(SIZE, opFile) - 1;
    fHandle->totalNumPages = numberOfPages(SIZE, opFile);
    free(memBlk);

    return response;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    RC response = checkFile(fHandle->fileName);
    if (response != RC_OK) {
        return response;
    }

    if (fHandle->totalNumPages > numberOfPages) {
        return RC_OK;
    }

    while (fHandle->totalNumPages < numberOfPages) {
        appendEmptyBlock(fHandle);
    }
    return response;
}