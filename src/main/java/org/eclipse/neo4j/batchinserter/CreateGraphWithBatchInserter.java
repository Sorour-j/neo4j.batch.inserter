package org.eclipse.neo4j.batchinserter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.io.layout.DatabaseLayout;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class CreateGraphWithBatchInserter {

	private static Path databaseDirectory = null;
	ResourceSet xmiResourceSet = new ResourceSetImpl();
	ResourceSet ecoreResourceSet = new ResourceSetImpl();
	Resource resource;
	ArrayList<String> className = new ArrayList<String>();
	ArrayList<String> refName = new ArrayList<String>();
	HashMap<String,Long> types = new HashMap<String,Long>();
	
	HashMap<Object, Long> visitedObjects = new HashMap<Object, Long>();
	BatchInserter inserter = null;
	RelationshipType relType,rel;
	// tag::vars[]
	GraphDatabaseService graphDb;

	public static void main(final String[] args) throws IOException {
		
		CreateGraphWithBatchInserter hello = new CreateGraphWithBatchInserter();
		
		String EcoreMetamodel = "model/JDTAST.ecore";
		String XmiModel = "model/Grabats-set1.xmi";
		
		hello.RegisterEcore(EcoreMetamodel);
		hello.loadXmi(XmiModel);
		hello.createDb();
		
		System.out.print("Return");
	}

	void createDb() throws IOException {
	
		//Node s, t, type;
		long sourceId, targetId, typeId;
		Map<String, Object> properties = new HashMap<>();
		
		databaseDirectory = Paths.get("target/Grabats-set1/data/databases/neo4j");
	    inserter = BatchInserters.inserter(DatabaseLayout.ofFlat(databaseDirectory));
		
		System.out.println("Starting graph creation ...");

		TreeIterator<EObject> it = resource.getAllContents();

		while (it.hasNext()) {
			EObject obj = it.next();
			
		//	EList<EObject> objects = resource.getContents();
		//	while (!objects.isEmpty()) {
		//		EObject obj = objects.get(0);
				
//				if (obj == null) 
//					return;
				
			//	objects.remove(obj);
//				if (!obj.eContents().isEmpty())
//					objects.addAll(obj.eContents());
//				
				String name = obj.eClass().getName();

				if (className.contains(name)) {
					
				
					if (!visitedObjects.keySet().contains(obj)) {
						
						sourceId = createNode(obj);
						
						if (!types.containsKey(name)) {
							
							properties.put("name", name);
							typeId = inserter.createNode(properties);
							types.put(name, typeId);
							//types.put(name,type);
						}
						else
							typeId = types.get(name);
						
						 relType = RelationshipType.withName("instanceOf");
					    inserter.createRelationship( sourceId, typeId, relType, null );
							
						visitedObjects.put(obj,sourceId);
					}
					else
						sourceId = visitedObjects.get(obj);

					for (EReference ref : obj.eClass().getEAllReferences()) {
						Object f =  obj.eGet(ref);
						
						if (f != null) {
						
						rel = RelationshipType.withName(ref.getName());
						ArrayList<EObject> refs = new ArrayList<EObject>();
						if (f instanceof Collection)
							refs.addAll((Collection<EObject>)f);
						else
							refs.add((EObject) f);
						
						for (EObject r : refs) {
							
							if (!visitedObjects.keySet().contains(r)) {
								
								targetId = createNode(r);
								if (!types.containsKey(r.eClass().getName())) {
									
									properties = new HashMap<String, Object>();
									properties.put("name", r.eClass().getName());
									typeId = inserter.createNode(properties);
									types.put(r.eClass().getName(), typeId);
									
								}
								else
									typeId = types.get(r.eClass().getName());
								
							
								 relType = RelationshipType.withName("instanceOf");
								 inserter.createRelationship( targetId, typeId, relType, null );

							}
							else
								targetId = visitedObjects.get(r);
							
							
							inserter.createRelationship( sourceId, targetId, rel, null );
							visitedObjects.put(r,targetId);
						}
						}
					}
				}
				
			}
			System.out.println("End!!..");
			try {
				inserter.shutdown();
			} catch (IllegalStateException ex) {
				// ignoring: already shutdown
			} catch (Exception e) {
				System.err.println("Shutdown failed!");
			}


	}

		long createNode(EObject obj) {
		long id;
		Map<String, Object> properties = new HashMap<>();
		List<Label> labels = new ArrayList<Label>();
		for (EAttribute atr : obj.eClass().getEAllAttributes()) {
			
			if (obj.eGet(atr) != null)
			properties.put(atr.getName(),obj.eGet(atr).toString());
		}
		
		labels.add(Label.label(obj.eClass().getName()));
		
		for (EClass c : obj.eClass().getEAllSuperTypes()) {
			labels.add(Label.label(c.getName()));
		}
		
		id = inserter.createNode(properties,labels.toArray(new Label[0]));
		return id;
	}
	
	void RegisterEcore(String metamodel) {
		System.out.println("Registering Ecore:......");
		ecoreResourceSet.getResourceFactoryRegistry().getExtensionToFactoryMap().put("*", new XMIResourceFactoryImpl());
		Resource ecoreResource = ecoreResourceSet
				.createResource(URI.createFileURI(new File(metamodel).getAbsolutePath()));
		try {
			ecoreResource.load(null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		for (EObject o : ecoreResource.getContents()) {
			EPackage ePackage = (EPackage) o;
			xmiResourceSet.getPackageRegistry().put(ePackage.getNsURI(), ePackage);
			for (EClassifier cls : ePackage.getEClassifiers()) {
				className.add(cls.getName());
			}
		}
		System.out.println("Registeration Done!......");
	}

	void loadXmi(String model) {
		System.out.println("Start loading xmi:......");
		xmiResourceSet.getResourceFactoryRegistry().getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());
		resource = xmiResourceSet.createResource(URI.createFileURI(new File(model).getAbsolutePath()));
		try {
			resource.load(null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Loading done!......");
	}
	
	void printInfo() {
		
		EList<EObject> objects = resource.getContents();
		while (!objects.isEmpty()) {
			EObject obj = objects.get(0);
			
			if (obj == null) 
				return;
			
			objects.remove(obj);
			if (!obj.eContents().isEmpty())
				objects.addAll(obj.eContents());

			String name = obj.eClass().getName();
			
			System.out.println("Label: " + obj.eClass().getName());
			if(obj.eClass().getName().equals("ClassDeclaration")) {
			for (EAttribute atr : obj.eClass().getEAllAttributes()) {
				//String value = obj.eGet(atr).toString();
				if (obj.eGet(atr) != null)
					System.out.println("Attr Name: "+ atr.getName() +" Attr value: " + obj.eGet(atr).toString());
			}
			for (EReference ref : obj.eClass().getEAllReferences()) {
				Object f =  obj.eGet(ref);
				System.out.println("Relationship Type: " + ref.getName());
				if (f != null) {
		
				ArrayList<EObject> refs = new ArrayList<EObject>();
				
				if (f instanceof Collection)
					refs.addAll((Collection<EObject>)f);
				else
					refs.add((EObject) f);
				
				for (EObject r : refs) {
					 System.out.println("Value Rel : " + r);
				}
				}
				else
					 System.out.println("Value Rel : Null");	
			}
	}
		}
}
}
