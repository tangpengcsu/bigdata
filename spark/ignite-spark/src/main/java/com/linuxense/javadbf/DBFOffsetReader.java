package com.linuxense.javadbf;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：
 * 模块描述：
 * 开发作者：tang.peng
 * 创建日期：2019/6/29
 * 模块版本：1.0.0.0
 * ----------------------------------------------------------------
 * 修改日期      版本       作者           备注
 * 2019/6/29   1.0.0.0    tang.peng     创建
 * ----------------------------------------------------------------
 */
public class DBFOffsetReader extends DBFReader {

    private boolean showDeletedRows = false;
    private DBFHeader header;
    private int currentOffset;
    private int startOffset;
    private int endOffset;
    private Map<String, Integer> mapFieldNames = new HashMap<String, Integer>();

    public DBFOffsetReader(InputStream in) {
        this(in,false);
    }

    public DBFOffsetReader(InputStream in, Boolean showDeletedRows) {
        this(in, null,showDeletedRows);
    }

    public DBFOffsetReader(InputStream in, Charset charset) {
        this(in, charset,false);
    }

    public DBFOffsetReader(InputStream in, Charset charset, boolean showDeletedRows) {
        super(in, charset, showDeletedRows);
        this.showDeletedRows = showDeletedRows;
        this.header = super.getHeader();
        this.mapFieldNames = createMapFieldNames(this.header.userFieldArray);
    }

    @Override
    public DBFRow nextRow() {
        Object[] record = nextRecord();
        if (record == null) {
            return null;
        }
        if (record.length == 0) {
            return nextRow();
        } else {
            return new DBFRow(record, mapFieldNames, this.header.fieldArray);
        }

    }
    public int partitionIdx;
    @Override
    public Object[] nextRecord() {
        if (currentOffset == endOffset) {
            return null;
        }
        Object[] result;
        try {
            try {
                if (currentOffset < startOffset) {
                    boolean isDeleted = false;
                    do {
                        try {
                            if (isDeleted && !showDeletedRows) {
                                skip(this.header.recordLength - 1);
                            }
                            int t_byte = this.dataInputStream.readByte();
                            if (t_byte == END_OF_DATA || t_byte == -1) {
                                return null;
                            }
                            isDeleted = t_byte == '*';
                        } catch (EOFException e) {
                            return null;
                        }
                    } while (isDeleted && !showDeletedRows);

                    skip(this.header.recordLength - 1);
                    return new Object[0];
                }
            } catch (IOException e) {
                throw new DBFException(e.getMessage(), e);
            }

            result = super.nextRecord();
        } finally {
            currentOffset++;
        }

        return result;
    }

    private Map<String, Integer> createMapFieldNames(DBFField[] fieldArray) {
        Map<String, Integer> fieldNames = new HashMap<String, Integer>();
        for (int i = 0; i < fieldArray.length; i++) {
            String name = fieldArray[i].getName();
            fieldNames.put(name.toLowerCase(), i);
        }
        return Collections.unmodifiableMap(fieldNames);
    }

   public DBFField[] getFields(){
        return this.header.userFieldArray;
   }
    public int getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(int startOffset) {
        this.startOffset = startOffset;
    }

    public int getCurrentOffset() {
        return currentOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(int endOffset) {
        this.endOffset = endOffset;
    }

    public Map<String, Integer> getMapFieldNames() {
        return mapFieldNames;
    }
}
