#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "codec.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#include "tbl.h"
#include "util.h"

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

#define MAX_PAGE_SIZE 4000


#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
#define CSV_NAME "data.csv"


/*
Takes a schema, and an array of strings (fields), and uses the functionality
in codec.c to convert strings into compact binary representations
 */
int
encode(Schema *sch, char **fields, byte *record, int spaceLeft) {
    //UNIMPLEMENTED;
    // for each field
    //    switch corresponding schema type is
    //        VARCHAR : EncodeCString
    //        INT : EncodeInt
    //        LONG: EncodeLong
    // return the total number of bytes encoded into record
    //Implemented as per instruction
    int num = sch ->numColumns, offset = 0;

    for (int i = 0; i < num; i++)
    {
        if (sch->columns[i]->type == INT)
        {
            int l = EncodeInt(atoi(fields[i]), record + offset);
            offset += l;
            spaceLeft -= l;
        }
        if (sch->columns[i]->type == LONG)
        {
            int l = EncodeLong((long long)fields[i], record + offset);
            offset += l;
            spaceLeft -= l;
        }
        if (sch->columns[i]->type == VARCHAR)
        {
            int l = EncodeCString(fields[i], record + offset, spaceLeft);
            offset += l;
            spaceLeft -= l;
        }
    }
    return offset;
}

Schema *
loadCSV() {
    // Open csv file, parse schema
    FILE *fp = fopen(CSV_NAME, "r");
    if (!fp) {
	perror("data.csv could not be opened");
        exit(EXIT_FAILURE);
    }

    char buf[MAX_LINE_LEN];
    char *line = fgets(buf, MAX_LINE_LEN, fp);
    if (line == NULL) {
	fprintf(stderr, "Unable to read data.csv\n");
	exit(EXIT_FAILURE);
    }

    // Open main db file
    Schema *sch = parseSchema(line);
    Table *tbl;

    checkerr(Table_Open(DB_NAME, sch, 1, &tbl));
    int fd = tbl->fd;
    PF_DestroyFile(INDEX_NAME);
    checkerr(AM_CreateIndex(DB_NAME, 0, 'i', 4));
    int indexFD = PF_OpenFile(INDEX_NAME);
    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL)
    {
        int n = split(line, ",", tokens);
        assert(n == sch->numColumns);
        int len = encode(sch, tokens, record, sizeof(record));
        RecId rid;

        Table_Insert(tbl, record, len, &rid); // Insert each record

        printf("%d %s\n", rid, tokens[0]);

        // Indexing on the population column
	// Indexing on the population column 
        // Indexing on the population column
        char *val = malloc(sizeof(int));
        EncodeInt(atoi(tokens[2]), val);
        int err = AM_InsertEntry(indexFD, 'i', 4, val, rid); // Using population field to index
        free(val);
        // UNIMPLEMENTED;
        //  Use the population field as the field to index on
        checkerr(err);
    }
    fclose(fp);
    Table_Close(tbl);
    int err = PF_CloseFile(indexFD);
    checkerr(err);
    return sch;
}

int main()
{
    loadCSV();
}
