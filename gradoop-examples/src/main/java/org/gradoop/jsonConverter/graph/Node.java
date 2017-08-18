package org.gradoop.jsonConverter.graph;

import java.util.ArrayList;
import java.util.Collections;

import org.gradoop.common.model.impl.id.GradoopId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings({ "unchecked", "serial" })

/**
 * data class to represent nodes as JSONObjects.
 *
 */

public class Node extends JSONObject {

	private JSONArray graphIdList = new JSONArray();
	private JSONObject data;
	private ArrayList<String> clusterIDs = new ArrayList<String>();
	final private String id = GradoopId.get().toString();

	/**
	 * constructor (for nodes of type metabolite)
	 * 
	 * @param oldId
	 *            node old id
	 * @param label
	 *            node label
	 * @param type
	 *            node type
	 */
	public Node(String oldId, String label, String type) {
		this(label, type);
		data.put("oldID", oldId);

	}

	/**
	 * constructor (for reaction and gene nodes)
	 * 
	 * @param label
	 *            node label
	 * @param type
	 *            node type
	 */
	public Node(String label, String type) {
		this.put("id", id);

		data = new JSONObject();
		data.put("type", type);

		data.put("ClusterId", "NoID");
		this.put("data", data);

		JSONObject meta = new JSONObject();
		meta.put("label", label);
		meta.put("graphs", graphIdList);
		this.put("meta", meta);
	}

	/**
	 * add {@code graphID}} to {@link graphIdList}
	 * 
	 * @param graphID
	 *            graphId to be added
	 */
	public void addGraph(String graphID) {
		if (!graphIdList.contains(graphID)) graphIdList.add(graphID);
	}

	/**
	 * add {@code strClusterID} to {@link ClusterID}
	 * 
	 * @param strClusterID
	 *            clusterId to be added
	 */
	public void setClusterID(String strClusterID) {
		if (strClusterID != null) {
			if (!clusterIDs.contains(strClusterID)) {
				clusterIDs.add(strClusterID);
				Collections.sort(clusterIDs);
				String strID = "";
				for (String currentID : clusterIDs) {
					strID = strID + "," + currentID;
				}
				strID = strID.substring(1);
				data.put("ClusterId", strID);
			}
		}
	}

	/**
	 * set quantity of {@code Nodes} in a compartment with {@code compartmentID}
	 * (for nodes of type metabolite)
	 * 
	 * @param compartmentID
	 *            id of compartment
	 * @param quantity
	 *            number of nodes in compartment
	 */
	public void setQuantityOfCompartment(String compartmentID, Integer quantity) {
		this.put(compartmentID, quantity);
	}

	/**
	 * 
	 * @return {@link id}
	 */
	public String getId() {
		return id;
	}

}
