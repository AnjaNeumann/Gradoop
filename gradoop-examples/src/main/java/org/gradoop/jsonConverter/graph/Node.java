package org.gradoop.jsonConverter.graph;

import java.util.ArrayList;
import java.util.Collections;

import org.gradoop.common.model.impl.id.GradoopId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


@SuppressWarnings({ "unchecked", "serial" })
public class Node extends JSONObject {

	private JSONArray m_LogicGraphsList = new JSONArray();
	private JSONObject data;
	private ArrayList<String> ClusterID = new ArrayList<String>();
	final private String strUUID = GradoopId.get().toString();

	public Node(String id, String label, String type) {
		this(label, type);
		data.put("oldID", id);

	}

	public Node(String label, String type) {
		super();
		this.put("id", strUUID);

		data = new JSONObject();
		data.put("type", type);
		
		// data.put("oldID", id);
		data.put("ClusterId", "NoID");
		this.put("data", data);

		JSONObject meta = new JSONObject();
		meta.put("label", label);
		meta.put("graphs", m_LogicGraphsList);
		this.put("meta", meta);
	}

	public void addGraph(String GraphID) {
		if (!m_LogicGraphsList.contains(GraphID))
			m_LogicGraphsList.add(GraphID);
	}

	public void setClusterID(String strClusterID) {
		if (strClusterID != null)
		{
			if (!ClusterID.contains(strClusterID))
			{
				ClusterID.add(strClusterID);
				Collections.sort( ClusterID);
				String strID = "";
				for(String currentID : ClusterID)
				{
					strID = strID + "," + currentID; 
				}
				
				strID = strID.substring(1);
				
				data.put("ClusterId", strID);
			}
		}
	}

	
	
	public void setQuantityOfCompartment(String compartmentID, Integer quantity) {
		this.put(compartmentID, quantity);
	}

	public String getStrUUID() {
		return strUUID;
	}

}
