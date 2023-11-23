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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

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
    public void postProcess() throws ObjectStoreException, IllegalAccessException {
        // query QTLs that have marker names
        Set<QTL> qtls = new HashSet<>();
        Query qQTL = new Query();
        QueryClass qcQTL = new QueryClass(QTL.class);
        qQTL.addFrom(qcQTL);
        qQTL.addToSelect(qcQTL);
        Results qtlResults = osw.getObjectStore().execute(qQTL);
        for (Object obj : qtlResults.asList()) {
            ResultsRow row = (ResultsRow) obj;
            QTL qtl = (QTL) row.get(0);
            if (qtl.getMarkerNames() != null) {
                qtls.add(qtl);
            }
        }
        LOG.info("Found " + qtls.size()+" QTL objects with marker names.");

        // collect lists of marker names from the string marker1|marker2|marker3|...
        // also store total set of names for query below
        Map<QTL,List<String>> qtlMarkerNames = new HashMap<>();
        Set<String> markerNames = new HashSet<>();
        for (QTL qtl : qtls) {
            List<String> names = Arrays.asList(qtl.getMarkerNames().split("\\|"));
            markerNames.addAll(names);
            qtlMarkerNames.put(qtl, names);
        }

        // query GeneticMarkers with name or alias matching marker name
        // there may be multiple GeneticMarkers on different genomes
        // NOTE: this will not return markers that have multiple comma-separated aliases (which are uncommon)
        Map<String,Set<GeneticMarker>> markerNameGeneticMarkers = new HashMap<>();
        for (String markerName : markerNames) {
            Query qMarkers = new Query();
            QueryClass qcMarkers = new QueryClass(GeneticMarker.class);
            qMarkers.addFrom(qcMarkers);
            qMarkers.addToSelect(qcMarkers);
            ConstraintSet constraints = new ConstraintSet(ConstraintOp.OR);
            constraints.addConstraint(new SimpleConstraint(new QueryField(qcMarkers, "name"), ConstraintOp.EQUALS, new QueryValue(markerName)));
            constraints.addConstraint(new SimpleConstraint(new QueryField(qcMarkers, "alias"), ConstraintOp.EQUALS, new QueryValue(markerName)));
            qMarkers.setConstraint(constraints);
            Set<GeneticMarker> markers = new HashSet<>();
            Results markerResults = osw.getObjectStore().execute(qMarkers);
            for (Object obj : markerResults.asList()) {
                ResultsRow row = (ResultsRow) obj;
                markers.add((GeneticMarker) row.get(0));
            }
            if (markers.size() > 0) {
                markerNameGeneticMarkers.put(markerName, markers);
            }
        }
        LOG.info("Found " + markerNameGeneticMarkers.size() + " marker names with at least one matching GeneticMarker.");

        // store the markers collections for QTLs
        int count = 0;
        osw.beginTransaction();
        for (QTL qtl : qtlMarkerNames.keySet()) {
            List<String> names = qtlMarkerNames.get(qtl);
            Set<GeneticMarker> markers = new HashSet<>();
            for (String name : names) {
                if (markerNameGeneticMarkers.containsKey(name)) markers.addAll(markerNameGeneticMarkers.get(name));
            }
            if (markers.size() > 0) {
                count++;
                QTL clone = PostProcessUtil.cloneInterMineObject(qtl);
                qtl.setMarkers(markers);
                osw.store(qtl);
            }
        }
        osw.commitTransaction();
        LOG.info("Stored markers collections for " + count + " QTL objects.");
    }
}
