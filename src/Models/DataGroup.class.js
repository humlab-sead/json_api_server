//NOTE: This class is not really meant to be instantiated, but rather just to serve as a reference for the structure of data groups
class DataGroup {
    data_group_id; //shall be equal to the dataset_id if this group represents a single dataset
	physical_sample_id;
	method_ids = [];
	values = [{
		analysis_entitity_id: 0,
		dataset_id, //if applicable
		key: '', 
		value: '',
		valueType: 'simple',
		data: {}, //if 'complex' value type
		methodId: 0,
	}]
}