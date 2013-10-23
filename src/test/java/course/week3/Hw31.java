package course.week3;

import com.mongodb.*;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: awagner
 * Date: 23.10.13
 * Time: 22:18
 * To change this template use File | Settings | File Templates.
 */
public class Hw31 {

    MongoClient mongoClient;
    DB schoolDatabase;
    DBCollection studentsCollection;

    @Before
    public void setup() throws UnknownHostException {
        mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost"));
        schoolDatabase = mongoClient.getDB("school");
        studentsCollection = schoolDatabase.getCollection("students");

    }


    @Test
    public void count() {

        long count = studentsCollection.count();
        assertThat(count, is(200L));

    }

    @Test
    public void homework() {
        DBCursor cursor = studentsCollection.find();

        while(cursor.hasNext()) {
            DBObject dbObject = cursor.next();
            BasicDBList scores = (BasicDBList) dbObject.get("scores");
            while(scores.iterator().hasNext()) {
                BasicDBObject score = ( BasicDBObject ) scores.iterator().next();
                    System.out.println(score);
                /*
                if(score.get("type").equals("homework")) {
                }
                */

            }



        }


    }


}
