package org.eclipse.neo4j.batchinserter;

import java.io.IOException;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.io.layout.DatabaseLayout;

public class Neo4jBatchInserter {
	
	private static Thread shutdownHook;
	static GraphDatabaseService graphDb ;
	static String db = "target/Test";
	static Path databaseDirectory = Paths.get(db);
	
	public static void main(final String[] args) throws IOException {
	BatchInserter inserter = null;
	
	try
	{
		//Create the Database
		DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(databaseDirectory)
				.setConfig( BoltConnector.enabled, true )
			    .setConfig( HttpConnector.enabled, true )
			    .setConfig(GraphDatabaseSettings.allow_upgrade, true)
			    .setConfig(GraphDatabaseSettings.fail_on_missing_files,false)
			    .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 7687 ) )
			    .build();
		
		GraphDatabaseService graphDb = managementService.database("neo4j");
		registerShutdownHook(managementService);
		managementService.shutdown();
		databaseDirectory = Paths.get(db + "/data/databases/neo4j");
		// Inserting to the created database
	    inserter = BatchInserters.inserter(DatabaseLayout.ofFlat(databaseDirectory));
	    System.out.println("Batch: " + inserter);
	    Label personLabel = Label.label( "Person" );
	    
	    Map<String, Object> properties = new HashMap<>();
	    List<Label> lbls = new ArrayList<Label>();
	    properties.put( "name", "Mattias" );
	    lbls.add(Label.label( "Person" ));
	    lbls.add(Label.label( "He" ));
	    long mattiasNode = inserter.createNode( properties, lbls.toArray(new Label[0]));
	    
	    properties = new HashMap<>();
	    lbls = new ArrayList<Label>();
	    properties.put( "name", "Chris" );
	    lbls.add(Label.label( "Person" ));
	    lbls.add(Label.label( "SHe" ));
	    long chrisNode = inserter.createNode( properties, lbls.toArray(new Label[0]));
	   
	   
	    RelationshipType knows = RelationshipType.withName( "KNOWS" );
	    inserter.createRelationship( mattiasNode, chrisNode, knows, null );
	    
	    System.out.println("Dir: " + inserter.getStoreDir());
	    System.out.println("Node: " + inserter.nodeExists(chrisNode));
	    System.out.println("Node: " + inserter.nodeExists(mattiasNode));
	}
	
	finally
	{
	    if ( inserter != null )
	    {
	        inserter.shutdown();
	    }
	}
	}
	protected static void registerShutdownHook(DatabaseManagementService managementService) {
		shutdownHook = new Thread() {
			@Override
			public void run() {
				//managementService.shutdownDatabase(DEFAULT_DATABASE_NAME);
				managementService.shutdown();
			}
		};
		Runtime.getRuntime().addShutdownHook(shutdownHook);
	}
	
}
