package store

type Version struct {
	Timestamp int64
	NodeID    string
}

type VersionedValue struct {
	Value     string
	Version   Version
	Tombstone bool
}

func (v1 Version) After(v2 Version) bool {
	if v1.Timestamp != v2.Timestamp {
		return v1.Timestamp > v2.Timestamp
	}
	return v1.NodeID > v2.NodeID
}

func (v1 Version) Equal(v2 Version) bool {
	return v1.Timestamp == v2.Timestamp && v1.NodeID == v2.NodeID
}

/*
"foo" -> VersionedValue{
    Value: "bar",
    Version: Version{
        Timestamp: 100,
        NodeID:    "node-a",
    },
    Tombstone: false,
}
*/
