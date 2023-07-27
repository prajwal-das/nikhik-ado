#ifndef COMMONS_H
#define COMMONS_H

#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <math.h>

#define MAX_PAGES_LIMIT 20000
#define MAX_FRAMES_LIMIT 200

static const size_t CHAR_SIZE = sizeof(char);
static const size_t CHAR_PAGE_SIZE = PAGE_SIZE * CHAR_SIZE;


static void closeFile(FILE *filePointer) {
    fclose(filePointer);
}

static int deleteFile(char *fileName) {
    int value = remove(fileName); //remove deletes the file and returns 0 if successful
    if (value != 0) {
        printf("Failed to delete the <%s>", fileName);
    } else {
        printf("File <%s> is destroyed successfully.", fileName);
    }
    return value;
}

static void writeToFile(char *block, size_t size, size_t count, FILE *file) {
    fwrite(block, size, count, file);
}

static int getTotalPages(FILE *file) {
    fseek(file, 0, SEEK_END); //fseek points the file pointer to the end of the file
    int totalPages = ftell(file) / PAGE_SIZE;
    rewind(file);
    return totalPages;
}

#endif //COMMONS_H