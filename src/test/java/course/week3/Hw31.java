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

        int count=0;
        while(cursor.hasNext()) {
            count++;
            DBObject dbObject = cursor.next();
            BasicDBList scores = (BasicDBList) dbObject.get("scores");
            System.out.println(dbObject.get("name"));

            double lowerScore = 0.0;
            int lowerIndex = 0;
            for(int i=0; i< scores.size();i++) {
                BasicDBObject score = (BasicDBObject) scores.get(i);
                String scoreType = score.getString("type");

                if(scoreType.equals("homework")) {
                    double scoreDouble = score.getDouble("score");
                    //lowerScore = scoreDouble;

                    if(Math.min(lowerScore, scoreDouble) == scoreDouble || lowerScore == 0.0) {
                        lowerScore = scoreDouble;
                        lowerIndex = i;
                        System.out.println("---"+i+ " - "+ lowerScore +" - "+scoreDouble);
                    }
                }

            }

            System.out.println("LOWERSCORE IS:"+ lowerIndex + " " + lowerScore);
            scores.remove(lowerIndex);
            studentsCollection.update(new BasicDBObject("_id",dbObject.get("_id")),dbObject);


            lowerScore = 0D;



        }
        System.out.println(count);


    }


}
