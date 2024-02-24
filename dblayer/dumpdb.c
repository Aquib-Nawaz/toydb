#include <stdio.h>
#include <stdlib.h>
#include "codec.h"
#include "tbl.h"
#include "util.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#define checkerr(err) {if (err < 0) {printf("%d,%s",err,#err) ; exit(1);}}


void
printRow(void *callbackObj, RecId rid, byte *row, int len) {
    Schema *schema = (Schema *) callbackObj;
    byte *cursor = row;

    //UNIMPLEMENTED;
    int num = schema ->numColumns, offset = 0;
    for (int i = 0; i<num; i++){
        if(schema->columns[i]->type == INT){
            int val = DecodeInt(cursor+offset);
            offset+=4;
            printf("%d",val);
        }
        if(schema->columns[i]->type == LONG){
            long long val = DecodeLong(cursor+offset);
            offset+=8;
            printf("%lld",val);
        }
        if(schema->columns[i]->type == VARCHAR){
            char *str = malloc((len-offset)*sizeof(char));
            int l = DecodeCString(row+offset,str,len-offset);
            offset +=l+2;                                   //l+2 for size(string) + size(length of string)
            printf("%s",str);
            free(str);
        }
        if(i!=num-1){
            printf(",");                                //comma seperated column
        }
        else{
            printf("\n");                               //newline seperated records
        }
    }
}

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
#define MAX_PAGE_SIZE 4000
	 
void
index_scan(Table *tbl, Schema *schema, int indexFD, int op, int value) {
    //UNIMPLEMENTED;
    char *val = malloc(sizeof(int));
    EncodeInt(value,val);
    int scanDesc = AM_OpenIndexScan(indexFD,'i',4,op,val);      //Open IndexScan
    checkerr(scanDesc);
    /*
    Open index ...
    while (true) {
	find next entry in index
	fetch rid from table
        printRow(...)
    }
    close index ...
    */
    int recId = AM_FindNextEntry(scanDesc);                     //First entry
    char record[MAX_PAGE_SIZE];
    while(recId!=AME_EOF){
        int len = Table_Get(tbl,recId,record,MAX_PAGE_SIZE);    //Get entry
        // if(len==-1) break;
        printRow((void *)schema,recId,record,len);              // Print entry
        recId = AM_FindNextEntry(scanDesc);                     //Find next entry
        //free(record);
    }
    free(val);
    checkerr(AM_CloseIndexScan(scanDesc));                      //Close
}

int
main(int argc, char **argv) {
    char *schemaTxt = "Country:varchar,Capital:varchar,Population:int";
    Schema *schema = parseSchema(schemaTxt);
    Table *tbl;

    Table_Open(DB_NAME,schema,0,&tbl);
    //UNIMPLEMENTED;
    if (argc == 1){
        printf("Provide 'i' or 's' \n");
        exit(1);
    }
    if (argc == 2 && *(argv[1]) == 's') {
        Table_Scan(tbl,(void*)tbl->schema,(ReadFunc)printRow);  //Sequential Scan
	//UNIMPLEMENTED;
	// invoke Table_Scan with printRow, which will be invoked for each row in the table.
    } else {
	// index scan by default
	int indexFD = PF_OpenFile(INDEX_NAME);      //Open index file
	checkerr(indexFD);

	// Ask for populations less than 100000, then more than 100000. Together they should
	// yield the complete database.
	index_scan(tbl, schema, indexFD, GREATER_THAN_EQUAL, 324459463);

    PF_CloseFile(indexFD); 

    }
    Table_Close(tbl);
}
