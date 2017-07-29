package org.swissbib.mf.mongo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.culturegraph.mf.framework.ObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.mongodb.MongoDBWriter;
import org.culturegraph.mf.mongodb.common.MongoDBConnection;
import org.culturegraph.mf.mongodb.common.MongoDBKeys;
import org.culturegraph.mf.mongodb.common.SimpleMongoDBConnection;
import org.culturegraph.mf.strings.StringReader;
import org.swissbib.mf.mongo.preprocessing.DefaultPreprocessing;

import java.io.ByteArrayOutputStream;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Reads records from a MongoDB collection and fetches the data from the special
 * record document field. Then some pre-processing has to be done in order to
 * provide the data to the receiver type which makes it special in relation the
 * basic DNB implementation {@link org.culturegraph.mf.mongodb.MongoDBReader}
 * <p>
 * MongoDBReader supports a simple query syntax to select records. Queries are
 * given as input strings with one query per line.
 * We have to evaluate if the simple query syntax is possible in our context
 * <p>
 * The query syntax is
 *
 * <pre>
 * [field:]value
 * </pre>
 *
 * A field is addressed as the literal name prefixed with the concatenation of
 * entity names starting from the root entity. If the field is
 * {@link MongoDBKeys#RECORD_ID_KEY} or is omitted, the record id will be
 * searched. Note that both entity and literal names must be prefixed with
 * {@link MongoDBKeys#KEY_PREFIX}.
 *
 * @author Günter Hipler
 */
@Description("reads single-line queries to retrieve records from a MongoDB collection. "
        + "Provide MongoDB access URI in brackets. "
        + "URI syntax: monogdb://user:pass@host:port/database.collection?options...")
@In(String.class)
@Out(StreamReceiver.class)
public class SwissbibMongoDBReader implements ObjectPipe<String, ObjectReceiver<String>> {

    private final MongoDBConnection mongoDBConnection;

    private ObjectReceiver<String> objectReceiver;


    /**
     * Creates an instance of {@code MongoDBReader}.
     *
     * @param uri {@code monogdb://user:pass@host:port/database.collection?options...}
     * @throws UnknownHostException if the IP address of the MongoDB server could
     * not be determined.
     */
    public SwissbibMongoDBReader(final String uri) throws UnknownHostException {
          mongoDBConnection = new SimpleMongoDBConnection(uri);
    }

    public SwissbibMongoDBReader(final MongoDBConnection mongoDBConnection) {
        this.mongoDBConnection = mongoDBConnection;
    }

    @Override
    public void process(String obj) {
        //final DBObject dbQuery = new BasicDBObject();
        //Bson filter = Filters.or(
        //        Filters.eq("status", "new"),
        //        Filters.eq("status", "updated")
        //);

        DBObject clause1 = new BasicDBObject("status", "new");
        DBObject clause2 = new BasicDBObject("status", "updated");
        BasicDBList or = new BasicDBList();
        or.add(clause1);
        or.add(clause2);
        DBObject query = new BasicDBObject("$or", or);


        final DBCursor dbCursor = mongoDBConnection.find(query);
        while (dbCursor.hasNext()) {
            final DBObject dbObject = dbCursor.next();
            Set<String> keySet = dbObject.keySet();
            byte[] data = (byte[])dbObject.get("record");
            String id =  (String)dbObject.get("_id");
            Optional<String> record = this.getDeCompressedRecord(data, id);
            if (record.isPresent()) {

                String unzippedRecord = record.get();
                String body = DefaultPreprocessing.cleanRecord(unzippedRecord);

                objectReceiver.process(body);
            }
        }

    }

    @Override
    public <R extends ObjectReceiver<String>> R setReceiver(R receiver) {
        objectReceiver = receiver;
        return receiver;
    }

    @Override
    public void resetStream() {
        objectReceiver.resetStream();

    }

    @Override
    public void closeStream() {
        objectReceiver.closeStream();
        mongoDBConnection.close();

    }


    private Optional<String> getDeCompressedRecord(byte[] zippedByteStream, String _id) {
        Optional<String> opt = Optional.empty();

        try {
            Inflater decompresser = new Inflater();
            decompresser.setInput(zippedByteStream);
            ByteArrayOutputStream bos = new ByteArrayOutputStream(zippedByteStream.length);
            byte[] buffer = new byte[8192];
            while (!decompresser.finished()) {
                int size = decompresser.inflate(buffer);
                bos.write(buffer, 0, size);
            }
            opt = Optional.of(bos.toString());
            decompresser.end();
        } catch (DataFormatException dfe) {
            //todo : what to do?

            dfe.printStackTrace();
        }

        return opt;
    }

}
