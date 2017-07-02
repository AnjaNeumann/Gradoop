package org.gradoop.metabolism;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.LogicalGraph;

import com.google.common.base.Preconditions;

public class Metabolism extends AbstractRunner {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Preconditions.checkArgument(
			      args.length == 1, "input dir required");
			    String inputDir  = args[0];
			    System.out.println(args[0]);


			    LogicalGraph graph = readLogicalGraph(inputDir);
			    System.out.println("read graph");
			    List<Edge> edges =graph.getEdges().collect();
			    List<Vertex> vertices = graph.getVertices().collect();
			    System.out.println("Edges: "+edges.size());
			    System.out.println("Vertices: "+vertices.size());
			    
			    Map<Vertex, Integer> EdgesInCounts = new HashMap<>();
			    Map<Vertex, Integer> EdgesOutCounts = new HashMap<>();
			    Set<GradoopId> idSet = new HashSet<>();
			    int cntIn, cntOut;
			    for (Vertex vertex : vertices){
			    	GradoopId vertexID = vertex.getId();
			    	//System.out.println(vertexID);
			    	
			    	cntIn = 0;
			    	cntOut = 0;
			    	for(Edge edge : edges){
			    		//System.out.println(edge.getSourceId());
			    		idSet.add(edge.getSourceId());
			    		if (edge.getSourceId().equals(vertexID)){
			    			//System.out.println("Source -> "+edge.getSourceId());
			    			cntOut ++;
			    		}
			    		if (edge.getTargetId().equals(vertexID)){
			    			//System.out.println("Target -> "+edge.getSourceId());
			    			cntIn ++;
			    		}
			    	}
			    	EdgesInCounts.put(vertex, cntIn);
			    	EdgesOutCounts.put(vertex, cntOut);
			    	System.out.println("Vertex: "+vertex.getLabel()+":\t incoming Edges: "+ cntIn +"\t outgoing Edges: "+ cntOut);
			    }
			    

	}
	
	

}
