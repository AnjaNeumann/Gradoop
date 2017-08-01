package jsonConverter.graph;

import org.gradoop.common.model.impl.id.GradoopId;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class LogicGraph extends JSONObject{

	final private String strUUID = GradoopId.get().toString();
	final private String strName;
	
	@SuppressWarnings("unchecked")
	public LogicGraph(String strLable, String strName, String strOldID) {
		super();
		this.strName = strName;
		this.put("id", strUUID);
		
		JSONObject data = new JSONObject();
		data.put("name", strName);
		
		if (strOldID != null)
		data.put("id", strOldID);
		
		this.put("data", data);
		
		JSONObject meta = new JSONObject();
		meta.put("label", strLable);
		this.put("meta", meta);
}

	
	
	public String getStrUUID() {
		return strUUID;
	}



	public String getStrName() {
		return strName;
	}
	
	
}
