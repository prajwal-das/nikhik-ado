#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

int buff_size = 0;
int l_indx = 0,count = 0;
int h_cnt = 0;
int clk_pntr = 0,lfu_pntr = 0;
RC Return_code;

extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
 Return_code=RC_OK;
    while(bm != NULL && pageFileName !=NULL){

        bm->pageFile=(char *)pageFileName;
        bm->strategy=strategy;
        bm->numPages=numPages;
        int size = sizeof(PageFrame);
        buff_size= numPages;
        int cnt=0;
        if(size== 0){
            return RC_PAGE_NOT_FOUND_ERROR;
        }
        PageFrame *pf=malloc(bm->numPages*size);  
        do{
            pf[cnt]=(PageFrame){.dirty_seg = 0,.fixCount = 0,.data=NULL,.index = 0,.pageNum = -1,.hit_cnt = 0};
            cnt++;
        }while(cnt<numPages);

        bm->mgmtData = pf;
        lfu_pntr = 0,clk_pntr = 0; 
        count = 0; 

        return Return_code;
    }
    return bm == NULL ? RC_BUFFER_POOL_NOT_FOUND : RC_FILE_NOT_FOUND;
}

extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
    PageFrame *pf = (PageFrame *)bm->mgmtData;
    if(forceFlushPool(bm) == RC_OK){
        int cntr=0;
        Top: 
            if(pf[cntr].fixCount == 0)
                NULL;
            else
                return RC_PINNED_PAGES_IN_BUFFER;
            if(!(cntr >=buff_size)){
                cntr++;
                goto Top;
            }
        bm->mgmtData = NULL;
    }
    free(pf);
    return RC_OK;
}

extern RC forceFlushPool(BM_BufferPool *const bm)
{

    while(bm!=NULL){
        PageFrame *pf = (PageFrame *)bm->mgmtData;
         SM_FileHandle fh;
        int cntr=0;
        while(cntr<buff_size && pf[cntr].dirty_seg < 1){
            if((! pf[cntr].fixCount!=0) && pf[cntr].dirty_seg ==0){
                openPageFile(bm->pageFile,&fh);
                int num = pf[cntr].pageNum;
                writeBlock(num,&fh,pf[cntr].data);
            }
        count++;
        pf[cntr].dirty_seg=0;
        cntr++;
        }
    return RC_OK;
    }
    return RC_BUFFER_POOL_NOT_FOUND;
}

extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const p)
{

    if(bm == NULL || p == NULL)
        return bm == NULL ? RC_BUFFER_POOL_NOT_FOUND : RC_PAGE_NOT_FOUND_ERROR;
    else{
        PageFrame *pf = (PageFrame *)bm->mgmtData;
        if(pf != NULL){
            for(int i=0;i<buff_size;i++){
                if(!(pf[i].pageNum <-1))
                    pf[i].dirty_seg=1;
            }
        }
    }
    return RC_OK;
}

extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const p)
{ 

    while(bm != NULL && p != NULL){
        PageFrame *pf = (PageFrame *)bm->mgmtData;
        int cntr =0;
        Top: 
            pf[cntr].fixCount = pf[cntr].pageNum == p->pageNum ? pf[cntr].fixCount-1 : pf[cntr].fixCount;
            cntr++;
            if(!(cntr == buff_size || cntr>buff_size))
            goto Top;

        return RC_OK;
    }

    return RC_PAGE_NOT_FOUND_ERROR;
}

extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const p)
{
    if(bm == NULL || p == NULL)
        return bm == NULL ? RC_BUFFER_POOL_NOT_FOUND : RC_PAGE_NOT_FOUND_ERROR;
    PageFrame *pf = (PageFrame*)bm->mgmtData; 
    Return_code=RC_OK;
    int cntr=0;
     SM_FileHandle fh;
    do{
        if(pf!= NULL && pf[cntr].pageNum == p->pageNum){
            while(openPageFile((*bm).pageFile, &fh) == RC_OK){
                int pg_num = pf[cntr].pageNum;
                writeBlock(pg_num, &fh, pf[cntr].data); 
                pf[cntr].dirty_seg = pf[cntr].dirty_seg < -1 ? pf[cntr].dirty_seg : 0;
                break;
            }
        }
        cntr++;
    }while(cntr < buff_size);
    return Return_code;
}

extern void FIFO(BM_BufferPool *const bm, PageFrame *p)
{
    int pos = l_indx % buff_size;
    if (bm != NULL &&  p!=NULL){
        
        PageFrame *pf = (PageFrame *)bm->mgmtData;
        SM_FileHandle fh;
        int cntr=0;
        Top: 
            if(pf[cntr].fixCount != 0){
                pos = (pos+1)%buff_size == 0 ? 0 : pos+1; 
            }
            else{
                while(pf[cntr].dirty_seg != 1){
                    if(openPageFile(bm->pageFile, &fh) == RC_OK)
                        writeBlock(pf[cntr].pageNum,&fh,pf[cntr].data);
                    count++;
                    break;
                }
                pf[cntr]=(PageFrame){.fixCount= p->fixCount,.data=p->data,.pageNum=p->pageNum,  
                    .hit_cnt=p->hit_cnt,.dirty_seg=p->dirty_seg
                    };
            }
            cntr++;
            while(cntr <buff_size)
                goto Top;

    }

    else
        NULL;
}

extern void LFU(BM_BufferPool *const bm, PageFrame *p)
{

    PageFrame *pf=(PageFrame*)bm->mgmtData;
    Return_code = RC_OK;
    int indx = lfu_pntr,lfu=0;
    SM_FileHandle fh;
    int cntr =0;
    do{
        lfu = pf[cntr].fixCount == 0 ? pf[(indx+cntr)%buff_size].index : lfu;

    }while(cntr <buff_size);
    cntr = (indx+1)%buff_size;
    for(int i=0;i<buff_size;i++){
        while(pf[cntr].index < lfu){

            int temp = indx + cntr;
            indx = temp % buff_size;
            lfu = pf[indx].index;
            goto Bot;
        }
        Bot: 
            cntr = (indx+1)%buff_size;
    }
    if(pf[indx].dirty_seg != 1){
            int temp = pf[indx].pageNum;
            pf[indx].pageNum = p-> pageNum;
            pf[indx].pageNum = temp;
        }
        else{
            if(openPageFile(bm->pageFile, &fh) == Return_code)
                writeBlock(pf[indx].pageNum,&fh,pf[indx].data);
            count = (count*2) +1 - count; 
        }
    lfu_pntr = indx*2 - indx +1;
    pf[indx]=(PageFrame){.fixCount=p->fixCount,.data=p->data,.pageNum=p->pageNum,  
                        .hit_cnt=p->hit_cnt,.dirty_seg=p->dirty_seg};
}

extern void LRU(BM_BufferPool *const bm, PageFrame *p)
{ 
    int cntr=0,l_hd =0, hd=0;
    PageFrame *pf = (PageFrame *) (*bm).mgmtData;
    SM_FileHandle fh;
    do{
        hd = pf[cntr].fixCount == 0 ? pf[cntr].hit_cnt : hd;
        l_hd = pf[cntr].fixCount == 0 ? cntr: l_hd;
        cntr++;
        break;
    }while(cntr <buff_size);

    cntr = l_hd +1;

    for(;cntr<buff_size;cntr++){

        hd = pf[cntr].hit_cnt< hd ? pf[cntr].hit_cnt : hd;
        l_hd = pf[cntr].hit_cnt< hd ? cntr : l_hd;

        }
    if(pf[l_hd].dirty_seg == 1)
        NULL;
    else{

        while(openPageFile(bm->mgmtData,&fh) == RC_OK){

            int pg_num = pf[l_hd].pageNum;
            writeBlock(pg_num,&fh,pf[l_hd].data);
            count = (count*2) +1 - count;
            break;
        }       
    }
    pf[l_hd]=(PageFrame){.fixCount=p->fixCount,.data=p->data,.pageNum=p->pageNum,  
                    .hit_cnt=p->hit_cnt,.dirty_seg=p->dirty_seg
                    };

}

 // To be done
extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const p, const PageNumber pageNum)
{
    Return_code=RC_OK;
    int temp=1;
    if(bm==NULL){
        temp=0;   
    }

    if(temp==0){
        return RC_BUFFER_POOL_NOT_FOUND;
    }
    else{
            if(p==NULL){
                return RC_PAGE_NOT_FOUND_ERROR;
            }
            else{
                    PageFrame *pf = (PageFrame *)(*bm).mgmtData;
                    if(pf[0].pageNum!=-1)
                    {
                        bool flag=true;
                        bool isBufferFull = true;
                        int temp=0; 

                        a:
                        if(!(temp>buff_size || temp==buff_size)){
                            if(pf[temp].pageNum==-1){
                                SM_FileHandle file_handle;
                                int pg=PAGE_SIZE;
                                if(pg==PAGE_SIZE)
                                    openPageFile((*bm).pageFile,&file_handle);
                                pf[temp]=(PageFrame){
                                    .data = (SM_PageHandle) malloc(pg)
                                };
                                pf[temp].pageNum = pageNum;

                                int zri=0;
                                if(pf[temp].pageNum == pageNum)
                                    readBlock(pageNum,&file_handle,pf[temp].data);
                                int cnt=1; 
                                h_cnt++; 
                                if(cnt==1)
                                    pf[temp].index = zri; 
                                pf[temp].fixCount = cnt; 

                                if(pf[temp].fixCount == cnt)
                                    l_indx++; 
                                
                                if(RS_LRU==(*bm).strategy){
                                    if(true)
                                        pf[temp].hit_cnt=h_cnt;
                                }
                                else if(RS_LRU==(*bm).strategy){
                                    if(true)
                                        pf[temp].hit_cnt=1;
                                }
                                isBufferFull=false;
                                    if(!isBufferFull)
                                        p->pageNum=pageNum;
                                    p->data=pf[temp].data;
                                    if(!isBufferFull)
                                        flag = false;
                            }
                            else{
                                if(pf[temp].pageNum==pageNum){
                                    isBufferFull = false;
                                    if(true)
                                        pf[temp].fixCount=pf[temp].fixCount+1;
                                    h_cnt++; 
                                    if((*bm).strategy==RS_CLOCK){
                                        if(true)
                                            pf[temp].hit_cnt=1;
                                    }
                                    else if((*bm).strategy==RS_LRU){
                                        pf[temp].hit_cnt = h_cnt;
                                    }
                                    else if((*bm).strategy==RS_LFU){
                                        if(true)
                                            pf[temp].index++;
                                    }

                                    flag=false;
                                    if(!isBufferFull)
                                        p->pageNum=pageNum;
                                    if(!isBufferFull)
                                        p->data=pf[temp].data;
                                    if(!isBufferFull)
                                        clk_pntr=clk_pntr+1;
                                }
                            }      
                        }   
            
            temp++;

            if(temp<buff_size)
                if(flag)
                    goto a;

            if(isBufferFull){
                SM_FileHandle file_handle;
                int temp=1;
                PageFrame *newPage=(PageFrame*)malloc(sizeof(PageFrame));
                if(temp==1)
                    openPageFile((*bm).pageFile,&file_handle);
                newPage->data=(SM_PageHandle)malloc(PAGE_SIZE);
                if(temp==1)
                    readBlock(pageNum,&file_handle,(*newPage).data);
                l_indx++;
                if(flag){
                    newPage->dirty_seg=0;
                    newPage->pageNum=pageNum;
                    newPage->index=0;
                    newPage->fixCount=1; 
                }
                h_cnt++;
                if(RS_CLOCK==(*bm).strategy){
                    (*newPage).hit_cnt=1;
                } 
                else if((*bm).strategy==RS_LRU){
                    (*newPage).hit_cnt=h_cnt;
                }
                p->data=newPage->data;
                p->pageNum=pageNum;
                switch ((*bm).strategy)
                {
                case RS_LRU:
                    if(true)
                        LRU(bm, newPage);
                    break;
                case RS_FIFO:
                    if(true)
                        FIFO(bm, newPage);
                    break;
                case RS_LFU:
                    if(true)
                        LFU(bm, newPage);
                    break;
                default:
                    break;  
            } 
        } 
        return Return_code; 
        }
        else{ 
            SM_FileHandle file_handle;
            if(true)
                openPageFile(bm->pageFile,&file_handle);
            int pp=PAGE_SIZE;
            if(pp == PAGE_SIZE)
                pf[0]=(PageFrame){
                .data=(SM_PageHandle) malloc(pp)
                };
            ensureCapacity(pageNum,&file_handle);
            if(true)
                readBlock(pageNum,&file_handle,pf[0].data);
            pf[0].fixCount=pf[0].fixCount+1+0;
            pf[0].pageNum=pageNum; 

            if(true)
                l_indx=0;
            
            if(h_cnt==0)
                h_cnt=0;    
            pf[0].index=0;
            if(pf[0].index == 0)
                pf[0].hit_cnt=h_cnt; 
            p->data=pf[0].data; 
            if(p->data==pf[0].data)
                p->pageNum=pageNum; 
        return Return_code; 
        } 
    }

 } 
}

extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    PageFrame *pf=(PageFrame *)bm-> mgmtData;
    PageNumber *container =malloc(sizeof(PageNumber)*buff_size);
    for(int i=0;i<buff_size;i++){
        container[i] = pf[i].pageNum < -1 ? pf[i].pageNum :  (pf[i].pageNum > -1 ? pf[i].pageNum : NO_PAGE);
    }
 return container;
}

extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
    PageFrame *pf = (PageFrame *)bm->mgmtData;
    while(buff_size > 0){
        int cntr=0;
        bool *dirty_Flg =  malloc(sizeof(bool) * buff_size);
        Top: 
            dirty_Flg[cntr] = pf[cntr].dirty_seg== 1 ? TRUE : FALSE;
            if(cntr < buff_size){
                cntr ++;
                goto Top;
            } 
            else
                return dirty_Flg;
    }
 return NULL;
}

extern int *getFixCounts (BM_BufferPool *const bm)
{
    int *fix_cnt = malloc(sizeof(int) * buff_size);
    PageFrame *pf= (PageFrame *)bm-> mgmtData;
    while(buff_size >0){
        int cntr =0;
        Top :
            fix_cnt[cntr] = pf[cntr].fixCount != -1 ? pf[cntr].fixCount : 0 ;
            if(cntr< buff_size){
                cntr++;
                goto Top;
            }
            else
            return fix_cnt;
    }
 return NULL;

}

extern int getNumReadIO (BM_BufferPool *const bm)
{
    if(bm == NULL)
        return RC_BUFFER_POOL_NOT_FOUND;
    int num = l_indx*2;
    l_indx++;
    if(num > 0)
        return (num/2) +1;
    else 
        return l_indx+1;

}

extern int getNumWriteIO (BM_BufferPool *const bm)
{
    return (count*2) == (count +count) ? count : 0; 
}
