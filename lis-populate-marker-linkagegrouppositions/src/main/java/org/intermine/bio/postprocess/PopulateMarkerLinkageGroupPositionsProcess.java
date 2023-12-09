package org.intermine.bio.postprocess;

/*
 * Copyright (C) 2002-2016 FlyMine, Legume Federation
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import java.util.Arrays;
import java.util.List;
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
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.model.bio.GeneticMarker;
import org.intermine.model.bio.LinkageGroupPosition;

import org.apache.log4j.Logger;

/**
 * Populate GeneticMarker.linkageGroupPositions by matching GeneticMarker.name to LinkageGroupPosition.markerName.
 * Note that a given genetic marker may have many distinct linkage group positions (from various experiments).
 *
 * @author Sam Hokin
 */
public class PopulateMarkerLinkageGroupPositionsProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateMarkerLinkageGroupPositionsProcess.class);

    /**
     * @param osw object store writer
     */
    public PopulateMarkerLinkageGroupPositionsProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        Query q = new Query();
        // 0 GeneticMarker
        QueryClass qcMarker = new QueryClass(GeneticMarker.class);
        q.addFrom(qcMarker);
        q.addToSelect(qcMarker);
        // 1 LinkageGroupPosition
        QueryClass qcLGP = new QueryClass(LinkageGroupPosition.class);
        q.addFrom(qcLGP);
        q.addToSelect(qcLGP);

	// GeneticMarker.name = LinkageGroupPosition.markerName
	QueryField geneticMarkerName = new QueryField(qcMarker, "name");
        QueryField lgpMarkerName = new QueryField(qcLGP, "markerName");
	SimpleConstraint sc = new SimpleConstraint(geneticMarkerName, ConstraintOp.EQUALS, lgpMarkerName);
	q.setConstraint(sc);

        // execute the query
        Results results = osw.getObjectStore().execute(q);
        // store a Set of LinkageGroupPositions in a Map keyed by GeneticMarker.id
        Map<Integer,GeneticMarker> markers = new HashMap<>();
        Map<Integer,Set<LinkageGroupPosition>> markerLGPs = new HashMap<>();
        int count = 0;
	for (Object resultObject : results.asList()) {
	    ResultsRow row = (ResultsRow) resultObject;
            GeneticMarker marker = (GeneticMarker) row.get(0);
            LinkageGroupPosition lgp = (LinkageGroupPosition) row.get(1);
            Integer id = marker.getId();
            if (markers.containsKey(id)) {
                markerLGPs.get(id).add(lgp);
            } else {
                markers.put(id, marker);
                Set<LinkageGroupPosition> lgps = new HashSet<>();
                lgps.add(lgp);
                markerLGPs.put(id, lgps);
            }
            count++;
	}
        LOG.info("Found " + count + " LinkageGroupPosition objects which map to " + markers.size() + " GeneticMarker objects.");

        // store updated GeneticMarker objects with the associated LinkageGroupPositions
	osw.beginTransaction();
        try {
            for (Integer id : markers.keySet()) {
                GeneticMarker marker = PostProcessUtil.cloneInterMineObject(markers.get(id));
                marker.setFieldValue("linkageGroupPositions", markerLGPs.get(id));
		osw.store(marker);
            }
        } catch (IllegalAccessException e) {
            throw new ObjectStoreException(e);
        }
        osw.commitTransaction();
        LOG.info("Stored " + markers.size() + " GeneticMarker.linkageGroupPositions collections.");
    }
}
