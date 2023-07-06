#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

RC ret_code;

static const size_t SIZE = sizeof(char);

/*
 * This function starts the storage manager. It prints a message indicating that the storage manager is being initialized.
 */
void initStorageManager(void) {
    printf("The Storage Manager is being initialized...\n");
}

int totalNumPages(int size, FILE *openFile) {
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

    char *memBlk = (char *) calloc(PAGE_SIZE, SIZE);
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
    fHandle->totalNumPages = totalNumPages(PAGE_SIZE, opFile);
    fHandle->mgmtInfo = opFile;
    fHandle->curPagePos = 0;
    fHandle->fileName = fileName;
    return response;
}

RC closePageFile(SM_FileHandle *fHandle) {
    RC response = checkFile(fHandle->fileName);
    return response;
}

RC destroyPageFile(char *fileName) {
    return remove(fileName) != 0 ? RC_FILE_NOT_FOUND : RC_OK;
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC response = checkFile(fHandle->fileName);

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
            totalNumPages(SIZE,
                          fh->mgmtInfo) - 1, fh, memPage);
}


RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    FILE *f = fopen(fHandle->fileName, "r+");
    /*Write contents to into the specified page.*/
    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_WRITE_FAILED;
    } else {
        if (fHandle != NULL) {
            int pageoffset = PAGE_SIZE * pageNum;
            if (fseek(f, pageoffset, SEEK_SET) == 0) {
                fwrite(memPage, sizeof(char), PAGE_SIZE, f);
                (fHandle->curPagePos = pageNum);
                fseek(f, 0, SEEK_END);
                int totalP = ftell(f) / PAGE_SIZE;
                (fHandle->totalNumPages = totalP);
                return RC_OK;
            } else {
                return RC_WRITE_FAILED;
            }
        } else {
            return RC_FILE_NOT_FOUND;
        }
    }

}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /* Taking the current position of the pointer, we write the data into that block. */
    if (fHandle != NULL) {
        int CurBlock = fHandle->curPagePos;
        return (writeBlock(CurBlock, fHandle, memPage));
    } else {
        return RC_FILE_NOT_FOUND;
    }
}


RC appendEmptyBlock(SM_FileHandle *fHandle) {
    FILE *f = fopen(fHandle->fileName, "r+");
    if (fHandle != NULL) {
        char *FreeBlock;
        FreeBlock = (char *) calloc(PAGE_SIZE, sizeof(char));
        fseek(f, 0, SEEK_END);
        if (fwrite(FreeBlock, 1, PAGE_SIZE, f) == 0)
            return RC_WRITE_FAILED;
        else {
            fHandle->totalNumPages = ftell(f) / PAGE_SIZE;
            fHandle->curPagePos = fHandle->totalNumPages - 1;
            free(FreeBlock);
            return RC_OK;
        }
    } else {
        return RC_FILE_NOT_FOUND;
    }
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } else {
        int pgs = numberOfPages - fHandle->totalNumPages;
        if (pgs < 0) {
            return RC_WRITE_FAILED;
        } else {
            for (int i = 0; i < pgs; i++)
                appendEmptyBlock(fHandle);
            return RC_OK;
        }
    }
}