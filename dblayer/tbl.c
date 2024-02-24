
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
    | numRecords | free ptr | recno | ptr | recno | ptr |  ....... | record last | |          | record1|
                                                                free
*/

int  getNumSlots(byte *pageBuf){
    return DecodeShort(pageBuf);            //First two byte
}

void setNumSlots(byte *pageBuf, int nslots){
    EncodeShort(nslots, pageBuf );          //First two byte of page is numslots
}

int  getNthSlotOffset(int slot, char* pageBuf){
    int numSlots = getNumSlots(pageBuf);    
    if(slot == -1)
        return PF_PAGE_SIZE;                //No slot in the page
    else if(slot >= numSlots)
        return -1;                          //Error if slot is greater than numslots
    else
        return DecodeShort(pageBuf+(2+slot)*SLOT_COUNT_OFFSET); //Slot offset array starts after two short
}

int  getLen(int slot, byte *pageBuf){
    int numRecord = getNumSlots(pageBuf);
    if(slot >= numRecord)
        return -1;
    return getNthSlotOffset(slot-1,pageBuf) - getNthSlotOffset(slot,pageBuf);   // slots are contagious so difference gives length
};

/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int
Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    
    // Initialize PF, create PF file,
    // allocate Table structure  and initialize and return via ptable
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage.
    PF_Init();
    if(overwrite){
        PF_DestroyFile(dbname);
    } 
    PF_CreateFile(dbname);                      //Creating file for table
    int fd = PF_OpenFile(dbname);               
    Table * table = malloc(sizeof(Table));
    table->schema = schema;
    table->fd = fd;                             //Storing file descriptor
    table->pageNum = 0;
    *ptable = table;
    return PFE_OK;
}

void
Table_Close(Table *tbl) {
    int pageNum = -1, fd=tbl->fd;
    byte *pg_buf;
    while(PF_GetNextPage(fd, &pageNum, &pg_buf) != PFE_EOF){
        checkerr(PF_UnfixPage(fd, pageNum, 0));
    }
    checkerr(PF_CloseFile(tbl->fd));
    // Unfix any dirty pages, close file.
}


int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
    int pageNum = tbl->pageNum-1, fd=tbl->fd;
    byte *pg_buf; int c=0;
    //Check if last page has enough space
    while((c=PF_GetNextPage(fd, &pageNum, &pg_buf)) != PFE_EOF &&
        DecodeShort(pg_buf+2)-SLOT_COUNT_OFFSET*(getNumSlots(pg_buf) + 2)<len+2){
        checkerr(PF_UnfixPage(fd, pageNum, 0));
    }
    //Add new page
    if(c==PFE_EOF){
        PF_AllocPage(fd, &pageNum, &pg_buf);
        setNumSlots(pg_buf, 0);
        EncodeShort(PF_PAGE_SIZE,pg_buf+2);
    }
    int numSlots = getNumSlots(pg_buf);
    int offset = DecodeShort(pg_buf+2)-len;
    EncodeShort(offset,SLOT_COUNT_OFFSET*(numSlots+2)+pg_buf);     // Writing offset for new record
    memcpy(pg_buf + offset, record, len);                          // Copying New Record
    setNumSlots(pg_buf,numSlots+1);                                
    EncodeShort(offset, pg_buf+2);                                 //Free Space Index
    PF_UnfixPage(fd, pageNum, 1);                              
    *rid = (((pageNum) << 16) | (numSlots&0xffff));                // Changing record ID
    tbl->pageNum = pageNum;                                        // store the pageNum of the page in which the entry is inserted 
    return PFE_OK;

}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    
    //Get slot and pageNum from record id
    
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;

    //Get the corresponding page

    int fd = tbl->fd;
    byte *pg_buf;
    PF_GetThisPage(fd,pageNum, &pg_buf);
    int glen = getLen(slot, pg_buf);
    int len = glen < maxlen ? glen : maxlen;
    
    //Check if slot exists
    if(len!=-1){
        int offset = getNthSlotOffset(slot, pg_buf);        //get slot's offset
        memcpy(record, pg_buf+offset, len);
    }

    PF_UnfixPage(fd, pageNum, 0);
    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    return len; // return size of record
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {

    //UNIMPLEMENTED;

    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
    int pageNum = -1, fd = tbl->fd, numr, len, offset, rid;
    byte *pg_buf;
    //For each page
    while(PF_GetNextPage(fd, &pageNum, &pg_buf) != PFE_EOF){
        numr = getNumSlots(pg_buf);
    // For each record
        for (int i=0; i<numr; i++){
            len = getLen(i, pg_buf);
            offset = getNthSlotOffset(i, pg_buf);
            rid = (((pageNum) << 16) | (i&0xffff));
            byte *record = malloc(len);             
            memcpy(record, pg_buf+offset, len);         //copy record
            callbackfn(callbackObj, rid, record, len);  //Callback function
            free(record);
        }
        checkerr(PF_UnfixPage(fd, pageNum, 0));
    }

    
}


