#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

RC ret_code;

void initStorageManager(void) {
    printf("Initializing Storage Manager...\n");
}

/* Creating a Page File */
RC createPageFile(char *fileName) {
    FILE *f = fopen(fileName, "w");
    char *memory_block = malloc(PAGE_SIZE * sizeof(char));
    if (f != NULL) {
        memset(memory_block, '\0', PAGE_SIZE);
        fwrite(memory_block, sizeof(char), PAGE_SIZE, f);
        /* free() is used to free memory blocks, so that no memory leakage occurs*/
        free(memory_block);
        fclose(f);
        return RC_OK;
    } else {
        free(memory_block);
        return RC_FILE_NOT_FOUND;
    }
}


RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    /*Open the created file*/
    FILE *f = fopen(fileName, "r+");
    if (f == NULL) {
        return RC_FILE_NOT_FOUND;
    } else {
        int sum_pages;
        fseek(f, 0, SEEK_END);
        int total_pgs = (int) (ftell(f) + 1) / PAGE_SIZE;
        fHandle->totalNumPages = total_pgs;
        fHandle->curPagePos = 0;
        fHandle->fileName = fileName;
        rewind(f);
        return RC_OK;
    }
}

RC closePageFile(SM_FileHandle *fHandle) {
    /*Check if file exists and close it. If not exists, throw error*/
    FILE *f = fopen(fHandle->fileName, "r+");
    if (f == NULL)
        return RC_FILE_NOT_FOUND;
    else {
        if (fclose(f) == 0)
            return RC_OK;
        else
            return RC_FILE_NOT_FOUND;
    }
}

RC destroyPageFile(char *Fname) {
    /*Delete the created file and free up the memory used.*/
    printf("Deletion of File Executed\n");
    char *mem_block = malloc(PAGE_SIZE * sizeof(char));
    free(mem_block);
    if (remove(Fname) ==
        0)                                       //If File gets destroyed return RC_OK or else return RT_FILE_NOT_FOUND
        return RC_OK;
    else
        return RC_FILE_NOT_FOUND;
}


RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /*Read the contents of the file present in specified page.*/
    int seekpage = pageNum * PAGE_SIZE;
    FILE *f = fopen(fHandle->fileName, "r+");
    if (f == NULL)
        return RC_FILE_NOT_FOUND;
    else {
        if (pageNum < 0 && pageNum > fHandle->totalNumPages)
            return RC_READ_NON_EXISTING_PAGE;
        else {
            fseek(f, seekpage, SEEK_SET);
            fread(memPage, sizeof(char), PAGE_SIZE, f);
            fHandle->curPagePos = pageNum;
            return RC_OK;
        }
    }
}

RC getBlockPos(SM_FileHandle *fHandle) {
    /* Find the pointer's current page position */
    if (fHandle != NULL) {
        return ((*fHandle).curPagePos);
    } else {
        return RC_FILE_NOT_FOUND;
    }
}


RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /*Read first block of file if file name exists */
    if (fHandle != NULL) {
        return (readBlock(0, fHandle, memPage));
    } else {
        return RC_FILE_NOT_FOUND;
    }
}


RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /* Using the present blocks poosition, we find the previous block and read it's comtents*/
    if (fHandle != NULL) {
        int PrevBlock = fHandle->curPagePos - 1;
        return (readBlock(PrevBlock, fHandle, memPage));
    } else {
        return RC_FILE_NOT_FOUND;
    }
}


RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /*Using the present blocks poosition, we read it's comtents*/
    if (fHandle != NULL) {
        int PrevBlock = fHandle->curPagePos - 1;
        return (readBlock(fHandle->curPagePos, fHandle, memPage));
    } else {
        return RC_FILE_NOT_FOUND;
    }

}


RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /*Using the present blocks poosition, we find the next block and read it's comtents*/
    if (fHandle != NULL) {
        int NextBlock = fHandle->curPagePos + 1;
        return (readBlock(NextBlock, fHandle, memPage));
    } else {
        return RC_FILE_NOT_FOUND;
    }
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    /*Read contents of the last page in the block*/
    if (fHandle != NULL) {
        int LastBlock = fHandle->totalNumPages - 1;
        return (readBlock(LastBlock, fHandle, memPage));
    } else {
        return RC_FILE_NOT_FOUND;
    }
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