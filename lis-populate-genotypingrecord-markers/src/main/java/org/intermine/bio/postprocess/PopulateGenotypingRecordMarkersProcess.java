package org.intermine.bio.postprocess;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryExpression;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.model.bio.GenotypingRecord;
import org.intermine.model.bio.GeneticMarker;

import org.apache.log4j.Logger;

/**
 * Populate GenotypingRecord.markers collections by matching GeneticMarker.name to markerName.
 *
 * @author Sam Hokin
 */
public class PopulateGenotypingRecordMarkersProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateGenotypingRecordMarkersProcess.class);

    /**
     * Populate a new instance of PopulateGenotypingRecordMarkersProcess
     *
     * @param osw object store writer
     */
    public PopulateGenotypingRecordMarkersProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        Query q = new Query();
	// 0
        QueryClass qcGenotypingRecord = new QueryClass(GenotypingRecord.class);
        q.addFrom(qcGenotypingRecord);
        q.addToSelect(qcGenotypingRecord);
	// 1
        QueryClass qcMarker = new QueryClass(GeneticMarker.class);
        q.addFrom(qcMarker);
        q.addToSelect(qcMarker);
	// genotypingRecord.markerName = marker.name
	QueryField genotypingRecordMarkerName = new QueryField(qcGenotypingRecord, "markerName");
	QueryField markerName = new QueryField(qcMarker, "name");
	SimpleConstraint sc = new SimpleConstraint(genotypingRecordMarkerName, ConstraintOp.EQUALS, markerName);
	q.setConstraint(sc);
        // execute the query
        Results results = osw.getObjectStore().execute(q);
        // store sets of GeneticMarkers in a map keyed by GenotypingRecord.id
        Map<Integer,Set<GeneticMarker>> genotypingRecordMarkers = new HashMap<>();
        // store the GenotypingRecords in a map keyed by GenotypingRecord.id
        Map<Integer,GenotypingRecord> genotypingRecords = new HashMap<>();
	for (Object resultObject : results.asList()) {
	    ResultsRow row = (ResultsRow) resultObject;
            GenotypingRecord genotypingRecord = (GenotypingRecord) row.get(0);
            GeneticMarker marker = (GeneticMarker) row.get(1);
            Integer id = genotypingRecord.getId();
            if (genotypingRecords.containsKey(id)) {
                genotypingRecordMarkers.get(id).add(marker);
            } else {
                genotypingRecords.put(id, genotypingRecord);
                Set<GeneticMarker> markers = new HashSet<>();
                markers.add(marker);
                genotypingRecordMarkers.put(id, markers);
            }
	}
        // store updated GenotypingRecords
	osw.beginTransaction();
        try {
            for (Integer id : genotypingRecords.keySet()) {
                GenotypingRecord genotypingRecord = PostProcessUtil.cloneInterMineObject(genotypingRecords.get(id));
                genotypingRecord.setFieldValue("markers", genotypingRecordMarkers.get(id));
		osw.store(genotypingRecord);
            }
        } catch (IllegalAccessException e) {
            throw new ObjectStoreException(e);
        }
        osw.commitTransaction();
        osw.close();
    }
}
