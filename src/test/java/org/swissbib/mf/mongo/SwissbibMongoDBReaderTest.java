package org.swissbib.mf.mongo;

import static org.mockito.Mockito.inOrder;

import com.mongodb.DBCursor;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.mongodb.common.MongoDBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.Assert.*;


public class SwissbibMongoDBReaderTest {

    @Mock
    private ObjectReceiver<String> receiver;

    private SwissbibMongoDBReader sbMongoDBReader;


    @Mock
    private MongoDBConnection mongoDBConnection;

    @Mock
    private DBCursor dbCursor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        sbMongoDBReader = new SwissbibMongoDBReader(mongoDBConnection);
        sbMongoDBReader.setReceiver(receiver);
    }




    @Test
    public void testFirst() {

        assertTrue(true);

    }




}
