package water.parser;


import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ArrayList;
import java.sql.Timestamp;

import water.Futures;
import water.Key;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.fvec.AppendableVec;
import water.util.Log;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

public class OrcParser {
    private Reader _reader;

/*  OrcParser( ParseSetup ps, Key jobKey ) { super(ps, jobKey); }

@Override
ParseWriter parseChunk(int cidx, final ParseReader din, final ParseWriter dout) { throw H2O.unimpl(); }

public static ParseSetup guessSetup( Key fkey ) { throw H2O.unimpl(); } */
    OrcParser(Key fkey) {
        Configuration conf = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.getLocal(conf);
            String pathStr = getPathForKey(fkey).replaceFirst("nfs://", "file:///");
            Path filePath = new Path(pathStr);
            reader = OrcFile.createReader(fileSystem, filePath);
         } catch (IOException ioe) {
            throw new H2OParseException("IO error trying open ORC file.");
         }
    }

    //Should final stats be compared to metadata stats?
    /**
     * Read all the contents of a stripe into the respective NewChunks. Each stripe
     * acts as a single parse chunk.
     *
     * @param newChunks - array of NewChunks to fill
     * @param stripeOffset -
     * @param stripeLength -
      */
    public void readStripe(NewChunk[] newChunks, long stripeOffset, long stripeLength) {
        ObjectInspector oi = _reader.getObjectInspector();
        // Root object is a single primitive type column
        if (oi.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            try {
                RecordReader rows = _reader.rowsOptions(new Reader.Options().range(stripeOffset, stripeLength));
                    readPrimitiveField(rows, oi, newChunks[0]);
                } catch (IOException ioe) {
                    Log.err(ioe);
                }
        } else { // Root object is a complex type with subfields
        //FIXME can the root object be a list or map?
            StructObjectInspector soi = (StructObjectInspector) oi;
            final List<? extends StructField> subfields = soi.getAllStructFieldRefs();
            final List<OrcProto.Type> orcColumns = _reader.getTypes(); //reflects on disk layout

            // Read data for each subfield
            for (int h2oIdx = 0, i = 0; i < subfields.size(); i++) {
                final StructField field = subfields.get(i);

                // As noted in https://issues.apache.org/jira/browse/HIVE-6632, HIVE cannot return
                // a single field from a complex data type, thus getORCColumnMap sets the column
                // list grab all relevant subfields at once
                boolean[] colInclude = getORCColumnMap(orcColumns, i);
                try {
                    RecordReader rows = _reader.rowsOptions(new Reader.Options().range(stripeOffset, stripeLength)
                            .include(colInclude));
                    ObjectInspector foi = field.getFieldObjectInspector();
                    if (foi.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                        readPrimitiveStructField(rows, field, foi, soi, newChunks[h2oIdx]);
                        h2oIdx++;
                    } else { //Complex field types can have several primitive inside (thus feed many H2O cols)
                        h2oIdx += readComplexStructField(rows, field, foi, soi, newChunks, h2oIdx);
                    }
                } catch (IOException ioe) {
                    Log.err(ioe);
                }
            }
        }
    }

    /**
     * Reads a stripe column made entirely of a single primitive type.
     *
     * @param rows - Iterator over data fields
     * @param oi - Object inspector for the primitive field
     * @param newChunk - NewChunk to fill with results
     */
    private void readPrimitiveField(RecordReader rows, ObjectInspector oi, NewChunk newChunk) {
        String typeString = oi.getTypeName();
        if (typeString.equals("boolean"))
            parseBoolColumn(rows, (BooleanObjectInspector) oi, newChunk);
        else if (typeString.equals("tinyint"))
            parseTinyIntColumn(rows, (ByteObjectInspector) oi, newChunk);
        else if (typeString.equals("smallint"))
            parseSmallIntColumn(rows, (ShortObjectInspector) oi, newChunk);
        else if (typeString.equals("int"))
            parseIntColumn(rows, (IntObjectInspector) oi, newChunk);
        else if (typeString.equals("bigint"))
            parseBigIntColumn(rows, (LongObjectInspector) oi, newChunk);
        else if (typeString.equals("float"))
            parseFloatColumn(rows, (FloatObjectInspector) oi, newChunk);
        else if (typeString.equals("double"))
            parseDoubleColumn(rows, (DoubleObjectInspector) oi, newChunk);
        else if (typeString.equals("string"))
            parseStringColumn(rows, (StringObjectInspector) oi, newChunk);
        else if (typeString.equals("binary"))
            parseBinaryColumn(rows, (BinaryObjectInspector) oi, newChunk);
        else if (typeString.equals("timestamp"))
            parseTimestampColumn(rows, (TimestampObjectInspector) oi, newChunk);
        else if (typeString.startsWith("decimal"))
            parseDecimalColumn(rows, (HiveDecimalObjectInspector) oi, newChunk);
        else if (typeString.equals("date"))
            parseDateColumn(rows, (DateObjectInspector) oi, newChunk);
        else throw new H2OParseException("encountered column type that can't be handled. "+ typeString);
    }

    /**
     *  Reads a primitive typed stripe column that is a field of a OrcStruct. These primitives are
     *  handle separately from the recursive calls of readComplexStructField() because they
     *  can be read in a tight loop.
     *
     * @param rows - Iterator over data fields
     * @param field - Field reference
     * @param foi - Object Inspector for desired field
     * @param soi - Object Inspector for the parent OrcStruct
     * @param newChunk - NewChunk to fill with results
     */
    private void readPrimitiveStructField(RecordReader rows, StructField field, ObjectInspector foi,
                                          StructObjectInspector soi, NewChunk newChunk) {
        String typeString = foi.getTypeName();
        if (typeString.equals("boolean"))
            parseBoolStructField(rows, field, soi, newChunk);
        else if (typeString.equals("tinyint"))
            parseTinyIntStructField(rows, field, soi, newChunk);
        else if (typeString.equals("smallint"))
            parseSmallIntStructField(rows, field, soi, newChunk);
        else if (typeString.equals("int"))
            parseIntStructField(rows, field, soi, newChunk);
        else if (typeString.equals("bigint"))
            parseBigIntStructField(rows, field, soi, newChunk);
        else if (typeString.equals("float"))
            parseFloatStructField(rows, field, soi, newChunk);
        else if (typeString.equals("double"))
            parseDoubleStructField(rows, field, soi, newChunk);
        else if (typeString.equals("string"))
            parseStringStructField(rows, field, soi, newChunk);
        else if (typeString.equals("binary"))
            parseBinaryStructField(rows, field, soi, newChunk);
        else if (typeString.equals("timestamp"))
            parseTimestampStructField(rows, field, soi, newChunk);
        else if (typeString.startsWith("decimal"))
            parseDecimalStructField(rows, field, soi, newChunk);
        else if (typeString.equals("date"))
            parseDateStructField(rows, field, soi, newChunk);
        else throw new H2OParseException("encountered column type that can't be handled. "+ typeString);
    }

    /**
     *  Reads a complex typed stripe column that is a field of a OrcStruct. Since the Hive library
     *  must return all (or none) data values from all child columns for a complex typed field, each
     *  row must parse each child value in order, one row at a time. Thanks Hive.
     *
     * @param rows - Iterator over data fields
     * @param field - Reference to the OrcStruct field to be read
     * @param foi - Object inspector for target field
     * @param soi - Object inspector for parent OrcStruct
     * @param newChunks - Array of NewChunks to hold new values
     * @param h2oIdx - Index of next NewChunk in array to fill
     * @return - The number of NewChunks filled from this field
     */
    private int readComplexStructField(RecordReader rows, StructField field, ObjectInspector foi,
                                       StructObjectInspector soi, NewChunk[] newChunks, int h2oIdx) {
        int primFieldTotal = 0;
            Object fieldData;
            Object prev = null;
            try {
                while (rows.hasNext()) {
                    Object row = rows.next(prev);
                    fieldData = soi.getStructFieldData(row, field);
                    primFieldTotal = parseField(fieldData, foi, newChunks, h2oIdx);
                    prev = row;
                }
            } catch(IOException ioe) {Log.err(ioe);}
                return primFieldTotal;
    }

     /**
      * The ORC columns are laid out in a flat form, despite the existence of recursive data
      * types.  Given the index of a root field, this returns an array showing which ORC column
      * indices correspond to the desired field. Since complex types can have several fields
      * within them, they will typically select more than one target ORC column.
      *
      * @param orcColList - A top-level type list for the ORC file
      * @param idx - The index of the desired StructField
      * @return - an array with an entry for each column in the ORC file, with the desired columns
      *          marked as TRUE.
      */
    private boolean[] getORCColumnMap(List<OrcProto.Type> orcColList, int idx) {
        final List<Integer> rootFieldColMap = orcColList.get(0).getSubtypesList();
        boolean[] colInclude = new boolean[orcColList.size()];
        int startIdx = rootFieldColMap.get(idx), stopIdx = orcColList.size();
        if (idx+1 != rootFieldColMap.size()) stopIdx = rootFieldColMap.get(idx+1);
            for(int i=startIdx; i< stopIdx;i++) colInclude[i] = true;
            return colInclude;
    }

      /**
       * Given an Field from a complex type, parse the field into the appropriate NewChunks, and
       * return a count of the number of offspring primitive fields parsed in the process.  Note
       * that due to the fact that List and Map type can contain a variable number of items per
       * row, they are reduced to strings.
       *
       * @param data - Data to be parsed
       * @param oi - Object inspector for the target data
       * @param newChunks - Array of NewChunks to fill with values
       * @param h2oIdx - Index of next NewChunk to fill
       * @return - The number of NewChunks filled from this field
       */
    private int parseField(final Object data, final ObjectInspector oi,NewChunk[] newChunks, int h2oIdx) {
        switch (oi.getCategory()) {
            case PRIMITIVE:
                return parsePrimitive(data, (PrimitiveObjectInspector) oi, newChunks, h2oIdx);
            case STRUCT:
                return parseStruct(data, (StructObjectInspector) oi, newChunks, h2oIdx);
            case LIST:
                case MAP:
                case UNION:
                if (data != null) {
                    newChunks[h2oIdx].addStr(data.toString());
                } else newChunks[h2oIdx].addNA();
                return 1;
            default:
                Log.err("Unknown Orc category");
                return 0;
        }
    }

        /**
         * Parse values of a primitive field inside an OrcStruct.
         *
         * @param data - Actual field data
         * @param oi - Object Inspector for the desired field
         * @param newChunks - array of NewChunks to hold values for this stripe
         * @param h2oIdx - index into the H2O NewChunk array for this field
         * @return 1 - the number of columns parsed
         */
    private int parsePrimitive(final Object data, final PrimitiveObjectInspector oi,
                        NewChunk[] newChunks, int h2oIdx) {
        String typeString = oi.getTypeName();
        Object jObj = oi.getPrimitiveJavaObject(data);
        if (typeString.equals("boolean")) {
        if (jObj != null) newChunks[h2oIdx].addNum((boolean) jObj ? 1 : 0, 0);
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("tinyint")) {
        if (jObj != null) newChunks[h2oIdx].addNum((byte) jObj);
        else newChunks[h2oIdx].addNA();
        }  else if (typeString.equals("smallint")) {
        if (jObj != null) newChunks[h2oIdx].addNum((short) jObj);
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("int")) {
        if (jObj != null) newChunks[h2oIdx].addNum((int) jObj);
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("bigint")) {
        if (jObj != null) newChunks[h2oIdx].addNum((long) jObj);
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("float")) {
        if (jObj != null) newChunks[h2oIdx].addNum((float) jObj);
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("double")) {
        if (jObj != null) newChunks[h2oIdx].addNum((double) jObj);
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("decimal")) {
        if (jObj != null) newChunks[h2oIdx].addNum(((HiveDecimal) jObj).doubleValue());
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("string")) {
        if (jObj != null) newChunks[h2oIdx].addStr(jObj);
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("binary")) {
        if (jObj != null) newChunks[h2oIdx].addStr("0x"+Hex.encodeHexString((byte[]) jObj));
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("timestamp")) {
        if (jObj != null) newChunks[h2oIdx].addNum(((Timestamp) jObj).getTime());
        else newChunks[h2oIdx].addNA();
        } else if (typeString.equals("date")) {
        if (jObj != null) newChunks[h2oIdx].addNum(((Date) jObj).getTime());
        else newChunks[h2oIdx].addNA();
        }
        return 1;
    }

    /**
    * Parse an OrcStruct. This walks through each field in the struct and parses them.
    *
    * @param data - Data to be parsed
    * @param soi - Object inspector for the target OrcStruct
    * @param newChunks - Array of NewChunks to fill with values
    * @param h2oIdx - Index of next NewChunk to fill
    * @return - The number of NewChunks filled from this field
    */
    private int parseStruct(final Object data, final StructObjectInspector soi,
                          NewChunk[] newChunks, int h2oIdx) {
    int primFieldTotal = 0;
    final List<? extends StructField> fields = soi.getAllStructFieldRefs();
    for(StructField field : fields) {
        final ObjectInspector fieldOI = field.getFieldObjectInspector();
        final Object fieldObj = soi.getStructFieldData(data, field);
        int primFieldCnt = parseField(fieldObj, fieldOI, newChunks, h2oIdx);
        h2oIdx += primFieldCnt;
        primFieldTotal += primFieldCnt;
    }
    return primFieldTotal;
    }

    /**
     * Parse boolean values from a Boolean StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseBoolStructField(RecordReader rows, StructField field, final StructObjectInspector soi, NewChunk nc) {
        BooleanObjectInspector boi = (BooleanObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                OrcStruct row = (OrcStruct) rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    boolean val = boi.get(soi.getStructFieldData(row, field));
                    nc.addNum(val ? 1 : 0, 0);
                } else nc.addNA();
                    prev = row;
                }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single Boolean column
     *
     * @param rows - Iterator over rows of data in column
     * @param boi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseBoolColumn(RecordReader rows, final BooleanObjectInspector boi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                    if (row != null) {
                        boolean val = boi.get(row);
                        nc.addNum(val ? 1 : 0, 0);
                    } else nc.addNA();
                        prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Parse byte values from a TinyInt StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseTinyIntStructField(RecordReader rows, StructField field,
    final StructObjectInspector soi, NewChunk nc) {
        ByteObjectInspector boi = (ByteObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    byte val = boi.get(soi.getStructFieldData(row, field));
                    nc.addNum(val, 0);
                } else nc.addNA();
                    prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single TinyInt (byte) column
     *
     * @param rows - Iterator over rows of data in column
     * @param boi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseTinyIntColumn(RecordReader rows, final ByteObjectInspector boi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (row != null) {
                    byte val = boi.get(row);
                    nc.addNum(val, 0);
                } else nc.addNA();
                    prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Parse short values from a SmallInt StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
    */
    private void parseSmallIntStructField(RecordReader rows, StructField field,
                    final StructObjectInspector soi, NewChunk nc) {
        ShortObjectInspector shoi = (ShortObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    short val = shoi.get(soi.getStructFieldData(row, field));
                    nc.addNum(val, 0);
                } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single SmallInt (short) column
     *
     * @param rows - Iterator over rows of data in column
     * @param soi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseSmallIntColumn(RecordReader rows, final ShortObjectInspector soi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (row != null) {
                    short val = soi.get(row);
                    nc.addNum(val, 0);
                } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {
            Log.err(ioe);
        }
    }

    /**
     * Parse int values from an Int StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseIntStructField(RecordReader rows, StructField field,
                                                 final StructObjectInspector soi, NewChunk nc) {
        IntObjectInspector ioi = (IntObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    int val = ioi.get(soi.getStructFieldData(row, field));
                    nc.addNum(val, 0);
                } else nc.addNA();
                    prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single Int column
     *
     * @param rows - Iterator over rows of data in column
     * @param ioi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseIntColumn(RecordReader rows, final IntObjectInspector ioi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
            Object row = rows.next(prev);
            if (row != null) {
                int val = ioi.get(row);
                nc.addNum(val, 0);
            } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {
            Log.err(ioe);
        }
    }

    /**
     * Parse long values from a BigInt StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseBigIntStructField(RecordReader rows, StructField field,
                                                     final StructObjectInspector soi, NewChunk nc) {
        LongObjectInspector loi = (LongObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    long val = loi.get(soi.getStructFieldData(row, field));
                    nc.addNum(val, 0);
                } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single BigInt (long) column
     *
     * @param rows - Iterator over rows of data in column
     * @param loi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseBigIntColumn(RecordReader rows, final LongObjectInspector loi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
            Object row = rows.next(prev);
            if (row != null) {
                long val = loi.get(row);
                nc.addNum(val, 0);
            } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {
            Log.err(ioe);
        }
    }

    /**
     * Parse float values from a Float StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseFloatStructField(RecordReader rows, StructField field,
                                           final StructObjectInspector soi, NewChunk nc) {
        FloatObjectInspector foi = (FloatObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    float val = foi.get(soi.getStructFieldData(row, field));
                    nc.addNum(val);
                } else nc.addNA();
                    prev = row;
                }
            } catch (IOException ioe) {Log.err(ioe);}
        }

    /**
     * Read entire stripe where the root item is a single Float column
     *
     * @param rows - Iterator over rows of data in column
     * @param foi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseFloatColumn(RecordReader rows, final FloatObjectInspector foi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (row != null) {
                    float val = foi.get(row);
                    nc.addNum(val);
                } else nc.addNA();
                    prev = row;
            }
        } catch (IOException ioe) {
            Log.err(ioe);
        }
    }

    /**
     * Parse double values from a Double StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseDoubleStructField(RecordReader rows, StructField field,
                                                     final StructObjectInspector soi, NewChunk nc) {
        DoubleObjectInspector doi = (DoubleObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    double val = doi.get(soi.getStructFieldData(row, field));
                    nc.addNum(val);
                } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single Double column
     *
     * @param rows - Iterator over rows of data in column
     * @param doi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseDoubleColumn(RecordReader rows, final DoubleObjectInspector doi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (row != null) {
                    double val = doi.get(row);
                    nc.addNum(val);
                } else nc.addNA();
                    prev = row;
            }
        } catch (IOException ioe) {
            Log.err(ioe);
        }
    }

    /**
     * Parse double values from a Decimal StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseDecimalStructField(RecordReader rows, StructField field,
                                                       final StructObjectInspector soi, NewChunk nc) {
        HiveDecimalObjectInspector hdoi = (HiveDecimalObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    HiveDecimal hd = hdoi.getPrimitiveJavaObject(soi.getStructFieldData(row, field));
                    if (hd != null) nc.addNum(hd.doubleValue());
                    else nc.addNA();
                } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single Decimal column
     *
     * @param rows - Iterator over rows of data in column
     * @param hdoi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseDecimalColumn(RecordReader rows, final HiveDecimalObjectInspector hdoi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (row != null) {
                    HiveDecimal val = hdoi.getPrimitiveJavaObject(row);
                    nc.addNum(val.doubleValue());
                } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {
            Log.err(ioe);
        }
    }

    /**
     * Parse string values from a String StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseStringStructField(RecordReader rows, StructField field,
                                      final StructObjectInspector soi, NewChunk nc) {
        StringObjectInspector stroi = (StringObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    String val = stroi.getPrimitiveJavaObject(soi.getStructFieldData(row, field));
                    if (val != null) nc.addStr(val);
                    else nc.addNA();
                } else nc.addNA();
                    prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single String column
     *
     * @param rows - Iterator over rows of data in column
     * @param soi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseStringColumn(RecordReader rows, final StringObjectInspector soi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (row != null) {
                    String val = soi.getPrimitiveJavaObject(row);
                    if (val != null) nc.addStr(val);
                    else nc.addNA();
                } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {
            Log.err(ioe);
        }
    }

    /**
     * Parse time values (milliseconds) from a Timestamp StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseTimestampStructField(RecordReader rows, StructField field,
                                              final StructObjectInspector soi, NewChunk nc) {
        TimestampObjectInspector toi = (TimestampObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    Timestamp val = toi.getPrimitiveJavaObject(soi.getStructFieldData(row, field));
                    if (val != null)  nc.addNum(val.getTime(), 0);
                    else nc.addNA();
                } else nc.addNA();
                    prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single Timestamp column
     *
     * @param rows - Iterator over rows of data in column
     * @param tsoi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseTimestampColumn(RecordReader rows, final TimestampObjectInspector tsoi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (row != null) {
                    Timestamp val = tsoi.getPrimitiveJavaObject(row);
                    if (val != null)  nc.addNum(val.getTime(), 0);
                    else nc.addNA();
                } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Parse time values (milliseconds) from a Date StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseDateStructField(RecordReader rows, StructField field,
                                    final StructObjectInspector soi, NewChunk nc) {
        DateObjectInspector doi = (DateObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    Date val = doi.getPrimitiveJavaObject(soi.getStructFieldData(row, field));
                    if (val != null) nc.addNum(val.getTime(), 0);
                    else nc.addNA();
                } else nc.addNA();
                prev = row;
            }
        } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single Date column
     *
     * @param rows - Iterator over rows of data in column
     * @param doi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseDateColumn(RecordReader rows, final DateObjectInspector doi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (row != null) {
                    Date val = doi.getPrimitiveJavaObject(row);
                    if (val != null) nc.addNum(val.getTime(), 0);
                    else nc.addNA();
                } else nc.addNA();
                    prev = row;
            }
        } catch (IOException ioe) {
            Log.err(ioe);
        }
    }

    /**
     * Parse string hex values from a Binary StructField into a NewChunk.  Reads entire
     * column in stripe into NewChunk.
     *
     * @param rows - Iterator over rows of data in the OrcStruct
     * @param field - Reference to the target StructField
     * @param soi - Object inspector for parent OrcStruct
     * @param nc - NewChunk to fill with values
     */
    private void parseBinaryStructField(RecordReader rows, StructField field,
                                            final StructObjectInspector soi, NewChunk nc) {
        BinaryObjectInspector boi = (BinaryObjectInspector) field.getFieldObjectInspector();
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                if (soi.getStructFieldData(row, field) != null) {
                    byte[] vals = boi.getPrimitiveJavaObject(soi.getStructFieldData(row, field));
                    nc.addStr("0x" + Hex.encodeHexString(vals));
                } else nc.addNA();
                    prev = row;
            }
         } catch (IOException ioe) {Log.err(ioe);}
    }

    /**
     * Read entire stripe where the root item is a single Binary (byte array) column. Bytes are
     * saved as a hex string in H2O.
     *
     * @param rows - Iterator over rows of data in column
     * @param boi - Object inspector for this column
     * @param nc - NewChunk to store value
     */
    private void parseBinaryColumn(RecordReader rows, final BinaryObjectInspector boi, NewChunk nc) {
        try {
            Object prev = null;
            while (rows.hasNext()) {
                Object row = rows.next(prev);
                    if (row != null) {
                        byte[] vals = boi.getPrimitiveJavaObject(row);
                        if (vals != null) nc.addStr("0x" + Hex.encodeHexString(vals));
                        else nc.addNA();
                    } else nc.addNA();
                    prev = row;
            }
        } catch (IOException ioe) {
            Log.err(ioe);
        }
    }

    private static class FieldInfo {
        public String _name; // needed since we make unique compound names from parent structures
        public byte _h2oType;
        public FieldInfo(String name, byte h2oType) {_name = name; _h2oType = h2oType;}
    }

    /**
     * Try to parse the file as an ORC file.
     *
     * @param fkey - Key to the absolute file name
     * @param bits - Initial bits of the file
     * @return ParseSetup that includes the column types, and other parse features of the file
     */
    public static ParseSetup guessSetup(Key fkey, byte[] bits) {
        if (!(bits[0] == 'O' && bits[1] == 'R' && bits[2] == 'C'))
            throw new H2OParseException("Could not parse file as an ORC file.");

        List<FieldInfo> fields;
        Key[] keys;
        AppendableVec[] avs;
        NewChunk[] ncs;
        Futures fs = new Futures();
        Vec[] tmpVecs;
        String[] colNames;
        byte[] colTypes;
        String[][] data;
        int colCnt;

         // find type information & init structures to read preview
        try {
            OrcParser parser = new OrcParser(fkey);
            if (parser._reader.getObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE) {
                fields = new ArrayList();
                // name the single field and figure out type information
                fields.add(new FieldInfo("C1",
                        convertOrcTypeToColumnType(parser._reader.getObjectInspector().getTypeName())));
            } else {  //FIXME handle categories other than struct
                // figure out type information
                StructObjectInspector readerInspector = (StructObjectInspector) parser._reader.getObjectInspector();
                fields = getFieldInfoList(readerInspector.getAllStructFieldRefs(), null);
            }
            colCnt = fields.size();
            colNames = new String[colCnt];
            colTypes = new byte[colCnt];
            // and build a frame to parse preview data into
            keys = Vec.VectorGroup.VG_LEN1.addVecs(fields.size());
            avs = new AppendableVec[colCnt];
            ncs = new NewChunk[colCnt];
            int i = 0;
            for (FieldInfo field : fields) {
                colNames[i] = field._name;
                colTypes[i] = field._h2oType;
                avs[i] = new AppendableVec(keys[i], field._h2oType);
                ncs[i] = new NewChunk(avs[i], 0);
                i++;
            }

            // get first stripe of data for preview
            parser.readStripe(ncs, 0, 1); //no idea how many bytes (guess 1, get whole 1st stripe)
            for (i = 0; i < colCnt; i++) ncs[i].close(0, fs);
                tmpVecs = AppendableVec.closeAll(avs, fs);
                fs.blockForPending();

                // Fill-in preview array
                data = fillPreviewColumn(tmpVecs, colNames, colTypes);

                // Cleanup
                for (i = 0; i < colCnt; i++) keys[i].remove();

                return new ParseSetup(ParserType.ORC, ParseSetup.GUESS_SEP, false, ParseSetup.HAS_HEADER,
                          colCnt, colNames, colTypes, null, null, data);
        } catch (Throwable t) {
            throw new H2OParseException("IO error trying open ORC file.");
        }
    }


    static private String[][] fillPreviewColumn(final Vec[] vs, String[] colNames, byte[] colTypes) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int rowCnt = (int) Math.min(vs[0].length(),10);
        String[][] data = new String[rowCnt][vs.length];
        for (int i =0; i < vs.length; i++) {
            switch (colTypes[i]) {
                case Vec.T_NUM:
                    for (int j=0; j < rowCnt; j++) data[j][i] = String.format("%g", vs[i].at(j));
                case Vec.T_STR:
                    for (int j=0; j < rowCnt; j++) data[j][i] = vs[i].atStr(new BufferedString(), j).toString();
                case Vec.T_TIME:
                    for (int j=0; j < rowCnt; j++) data[j][i] = sdf.format(vs[i].at8(j));
                        default: // No other column types are currently made
            }
        }

        return data;
    }

    private static List<FieldInfo> getFieldInfoList(List<? extends StructField> fields, String parentPrefix) {
        List<FieldInfo> stuctFields = new ArrayList<>();
        try {
            for (StructField field : fields) {
                String orcFieldType = field.getFieldObjectInspector().getTypeName();
                byte h2oFieldType = convertOrcTypeToColumnType(orcFieldType);
                String prefix;
                if (orcFieldType.startsWith("struct")) {
                    if (parentPrefix != null && !parentPrefix.equals("")) prefix = parentPrefix + "_" + field.getFieldName();
                    else prefix = field.getFieldName();
                    stuctFields.addAll(getFieldInfoList(((StructObjectInspector) field.getFieldObjectInspector()).getAllStructFieldRefs(), prefix));
                } else if (orcFieldType.startsWith("array") || orcFieldType.startsWith("map") || orcFieldType.startsWith("uniontype")) {
                    if (parentPrefix != null && !parentPrefix.equals("")) prefix = parentPrefix + "_" + field.getFieldName();
                    else prefix = field.getFieldName();
                    stuctFields.add(new FieldInfo(prefix, h2oFieldType));
                } else { // a primitive type
                    if (parentPrefix != null && !parentPrefix.equals("")) // part of a complex type
                        stuctFields.add(new FieldInfo(parentPrefix+ "_"+field.getFieldName(), h2oFieldType));
                    else
                        stuctFields.add(new FieldInfo(field.getFieldName(), h2oFieldType));
                }
            }
        } catch(ClassCastException cce) {
            Log.warn(cce);
        }
        return stuctFields;
    }

    private static byte convertOrcTypeToColumnType(String orcTypeStr) {
        if (orcTypeStr.equals("boolean") || orcTypeStr.equals("tinyint")
                    || orcTypeStr.equals("smallint") || orcTypeStr.equals("int")
                      || orcTypeStr.equals("bigint") || orcTypeStr.equals("float")
                     || orcTypeStr.equals("double") || orcTypeStr.startsWith("decimal") ) return Vec.T_NUM;

        if (orcTypeStr.equals("string"))  return Vec.T_STR;
        if (orcTypeStr.equals("binary")) return Vec.T_STR;
        if (orcTypeStr.equals("timestamp") || orcTypeStr.equals("date")) return Vec.T_TIME;
        if (orcTypeStr.startsWith("array") || orcTypeStr.startsWith("map") || orcTypeStr.startsWith("union")) return Vec.T_STR;
        //FIXME should be an assert
        if (orcTypeStr.startsWith("struct")) return Vec.T_BAD;

        Log.err("Encountered unhandled column type in Hive file: "+orcTypeStr);
        return Vec.T_BAD;
    }

    public int getStripeCount() {
        return _reader.getStripes().size();
    }

    public long[] getEspc() {
        List<StripeInformation> stripes = _reader.getStripes();
        int count = stripes.size();
        long[] espc = new long[count+1];
        int i = 1;
        for (StripeInformation info : stripes) espc[i++] = info.getNumberOfRows();
        for (i = 1; i < count+1; i++) espc[i] += espc[i-1];
        return espc;
    }

    public long[] getStripeOffsets() {
        List<StripeInformation> stripes = _reader.getStripes();
        int count = stripes.size();
        long[] offsets = new long[count];
        int i = 0;
        for(StripeInformation info : stripes) offsets[i++] = info.getOffset();
        return offsets;
    }

    public long[] getStripeLengths() {
        List<StripeInformation> stripes = _reader.getStripes();
        int count = stripes.size();
        long[] lengths = new long[count];
        int i = 0;
        for(StripeInformation info : stripes) lengths[i++] = info.getLength();
        return lengths;
    }

    public long[] getDataLengths() {
        List<StripeInformation> stripes = _reader.getStripes();
        int count = stripes.size();
        long[] lengths = new long[count];
        int i = 0;
        for(StripeInformation info : stripes) lengths[i++] = info.getDataLength();
        return lengths;
    }

    public long[] getNumberOfRows() {
        List<StripeInformation> stripes = _reader.getStripes();
        int count = stripes.size();
        long[] numRows = new long[count];
        int i = 0;
        for(StripeInformation info : stripes) numRows[i++] = info.getNumberOfRows();
        return numRows;
    }

    // Returns String with path for given key.
    // FIXME stolen from permit-hdfs, set both files to use a common source
    public static String getPathForKey(Key k) {
        final int off = k._kb[0]==Key.VEC ? Vec.KEY_PREFIX_LEN : 0;
        return new String(k._kb,off,k._kb.length-off);
    }
}