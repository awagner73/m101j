package course.week2.hw22;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.GroupCommand;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class HW2_2 {
	
	MongoClient mongoClient;
    DB studentsDatabase;
    DBCollection gradesCollection;
    
    @Before
    public void setup() throws UnknownHostException {
    	mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost"));
    	studentsDatabase = mongoClient.getDB("students");  
    	gradesCollection = studentsDatabase.getCollection("grades");
    	
    }
	

	@Test
	public void count() {
		
		long count = gradesCollection.count();
		assertThat(count, is(800L));
		
	}
	
	
	
	@Test
	//@Ignore
	public void getOnlyHotmework() {
		
		/*
		 * db.grades.aggregate([{$match:{type:"homework"}},{$group:{_id:"$student_id",score:{$min:"$score"}}}])
		 * 	{ $sort: { count: -1 } }
		 */
		
		/*
		DBObject match = new BasicDBObject("$match", new BasicDBObject("type", "airfare") );
		DBObject groupFields = new BasicDBObject( "_id", "$department");
		groupFields.put("average", new BasicDBObject( "$avg", "$amount"));
		DBObject group = new BasicDBObject("$group", groupFields);
		*/
		
		DBObject matchOp = new BasicDBObject("$match",new BasicDBObject("type","homework"));
		
//		DBObject groupFields = new BasicDBObject( "_id", "$_id");
//		DBObject groupFields = new BasicDBObject( "_id", "$student_id");
		DBObject groupFields = new BasicDBObject( "_id", "$student_id");
		groupFields.put("score", new BasicDBObject("$min","$score"));
		
		DBObject groupOp = new BasicDBObject("$group",groupFields);		
		DBObject sortOp = new BasicDBObject("$sort",new BasicDBObject("_id",1));
		
		AggregationOutput output = gradesCollection.aggregate(matchOp,groupOp,sortOp);
		
		System.out.println(output);
		Iterator<DBObject> iterator = output.results().iterator();
		int i=0;
		while (iterator.hasNext()) {
			DBObject dbObject = (DBObject) iterator.next();
			System.out.println(dbObject.get("_id") + " - " + dbObject.get("score"));
			//gradesCollection.remove(dbObject);
			
//			BasicDBObject rem = new BasicDBObject("student_id",dbObject.get("_id"));
//			rem.put("score", dbObject.get("score"));
//			rem.put("type", "homework");
//			gradesCollection.remove(rem);
			
			
			i++;
			
		}
		System.out.println("["+i+"]");
		
		System.out.println("[["+gradesCollection.count()+"]]");
		
		
	}
	
	@Test
	public void findOne() {
		BasicDBObject  query = new BasicDBObject("student_id",199);
		DBCursor cursor = gradesCollection.find(query);

		try {
		   while(cursor.hasNext()) {
		       System.out.println(cursor.next());
		   }
		} finally {
		   cursor.close();
		}
		
	}

}
