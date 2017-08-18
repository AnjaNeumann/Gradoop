package org.gradoop.jsonConverter.graph;

import org.gradoop.common.model.impl.id.GradoopId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings({ "unchecked", "serial" })

/**
 * data class to represent edges as JSONObjects.
 *
 */
public class Edge extends JSONObject {

	JSONArray graphIdList = new JSONArray();
	final private String id = GradoopId.get().toString();

	/**
	 * constructor (for edges from gene to reaction nodes)
	 * 
	 * @param source
	 *            id of source node
	 * @param target
	 *            id of target node
	 * @param label
	 *            edge label
	 */
	public Edge(String source, String target, String label) {
		this(source, target, null, label);

	}

	/**
	 * constructor (for edges between reaction and metabolite nodes)
	 * 
	 * @param source
	 *            id of source node
	 * @param target
	 *            id of target node
	 * @param quantity
	 *            number of the connected metabolite (e.g. ATP) the reaction
	 *            needs as input or gives as output
	 * @param label
	 *            edge label (input or output)
	 */
	public Edge(String source, String target, Double quantity, String label) {
		this.put("id", id);
		this.put("target", target);
		this.put("source", source);

		JSONObject dataObj = new JSONObject();
		if (quantity != null) dataObj.put("quantity", quantity);
		this.put("data", dataObj);

		JSONObject metaObj = new JSONObject();
		metaObj.put("label", label);
		metaObj.put("graphs", graphIdList);
		this.put("meta", metaObj);
	}

	/**
	 * add {@code graphID}} to {@link graphIdList}
	 * 
	 * @param graphID
	 *            graphId to be added
	 */
	public void addGraph(String GraphID) {
		if (!graphIdList.contains(GraphID)) graphIdList.add(GraphID);
	}

	/**
	 * 
	 * @return {@link id}
	 */
	public String getId() {
		return id;
	}

}