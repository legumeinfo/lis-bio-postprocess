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

import java.util.List;
import java.util.ArrayList;
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
import org.intermine.objectstore.query.MultipleInBagConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.intermine.model.bio.QTL;
import org.intermine.model.bio.GeneticMarker;

import org.apache.log4j.Logger;

/**
 * Populate QTL.markers collections by matching GeneticMarker.name or GeneticMarker.alias to markerName.
 *
 * @author Sam Hokin
 */
public class PopulateQTLMarkersProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateQTLMarkersProcess.class);

    /**
     * Populate a new instance of PopulateQTLMarkersProcess
     *
     * @param osw object store writer
     */
    public PopulateQTLMarkersProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        // query QTLs and collect sets of marker names
	// QTL.markerNames = marker1|marker2|marker3|...
        Map<Integer,QTL> qtls = new HashMap<>();
        Map<Integer,Set<String>> qtlMarkerNames = new HashMap<>();
        Query qQTL = new Query();
        QueryClass qcQTL = new QueryClass(QTL.class);
        qQTL.addFrom(qcQTL);
        qQTL.addToSelect(qcQTL);
        Results resultsQTL = osw.getObjectStore().execute(qQTL);
        for (Object qtlObject : resultsQTL.asList()) {
            ResultsRow qtlRow = (ResultsRow) qtlObject;
            QTL qtl = (QTL) qtlRow.get(0);
            if (qtl.getMarkerNames()!=null) {
                int qtlId = qtl.getId();
                qtls.put(qtlId, qtl);
                Set<String> markerNames = new HashSet<>();
                for (String markerName : qtl.getMarkerNames().split("\\|")) {
                    markerNames.add(markerName);
                }
                qtlMarkerNames.put(qtlId, markerNames);
            }
        }
        System.err.println("Found "+qtlMarkerNames.size()+" QTLs with marker names.");
        // query Genetic Markers with name or alias matching QTL markerNames and store in a set per QTL
        // NOTE: this will not return markers that have multiple comma-separated aliases (which are uncommon)
        Map<Integer,Set<GeneticMarker>> qtlMarkers = new HashMap<>();
        for (int qtlId : qtlMarkerNames.keySet()) {
            Set<String> markerNames = qtlMarkerNames.get(qtlId);
            Query qMarkers = new Query();
            QueryClass qcMarkers = new QueryClass(GeneticMarker.class);
            qMarkers.addFrom(qcMarkers);
            qMarkers.addToSelect(qcMarkers);
            List<QueryField> queryFields = new ArrayList<>();
            queryFields.add(new QueryField(qcMarkers, "name"));
            queryFields.add(new QueryField(qcMarkers, "alias"));
            qMarkers.setConstraint(new MultipleInBagConstraint(markerNames, queryFields));
            Set<GeneticMarker> markers = new HashSet<>();
            Results resultsMarkers = osw.getObjectStore().execute(qMarkers);
            for (Object markerObject : resultsMarkers.asList()) {
                ResultsRow markerRow = (ResultsRow) markerObject;
                markers.add((GeneticMarker) markerRow.get(0));
            }
            if (markers.size()>0) {
                qtlMarkers.put(qtlId, markers);
            }
        }
        System.err.println("Found "+qtlMarkers.size()+" QTLs with at least one matching GeneticMarker.");
        // store the markers collection for QTLs for which we've found GeneticMarkers
        osw.beginTransaction();
        for (int qtlId : qtlMarkers.keySet()) {
            try {
                QTL qtl = PostProcessUtil.cloneInterMineObject(qtls.get(qtlId));
                Set<GeneticMarker> markers = qtlMarkers.get(qtlId);
                qtl.setMarkers(markers);
                osw.store(qtl);
            } catch (IllegalAccessException ex) {
                System.err.println(ex.toString());
                System.exit(1);
            }
        }
        osw.commitTransaction();
    }
}
