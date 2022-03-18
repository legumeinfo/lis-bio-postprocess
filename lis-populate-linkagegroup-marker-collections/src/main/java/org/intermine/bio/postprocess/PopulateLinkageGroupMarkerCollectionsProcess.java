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
import org.intermine.objectstore.query.BagConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.intermine.model.bio.LinkageGroup;
import org.intermine.model.bio.GeneticMarker;

import org.apache.log4j.Logger;

/**
 * Populate LinkageGroup.markers collections by matching GeneticMarker.name to markerName.
 *
 * @author Sam Hokin
 */
public class PopulateLinkageGroupMarkerCollectionsProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateLinkageGroupMarkerCollectionsProcess.class);

    /**
     * Populate a new instance of PopulateLinkageGroupMarkerCollectionsProcess
     *
     * @param osw object store writer
     */
    public PopulateLinkageGroupMarkerCollectionsProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        Query qLG = new Query();
	// linkageGroup.markerNames = marker1|marker2|marker3|...
        QueryClass qcLG = new QueryClass(LinkageGroup.class);
        qLG.addFrom(qcLG);
        qLG.addToSelect(qcLG);
        // execute the outer query on linkage groups
        Results resultsLG = osw.getObjectStore().execute(qLG);
        for (Object lgObject : resultsLG.asList()) {
            ResultsRow lgRow = (ResultsRow) lgObject;
            LinkageGroup lg = (LinkageGroup) lgRow.get(0);
            // TOG908354_517|TOG916151_413|TOG903841_238|TOG905314_471|TOG908354_43
            if (lg.getMarkerNames()!=null) {
                List<String> markerNames = Arrays.asList(lg.getMarkerNames().split("\\|"));
                // GeneticMarker.name IN markerNames into Set
                Query qMarkers = new Query();
                QueryClass qcMarkers = new QueryClass(GeneticMarker.class);
                qMarkers.addFrom(qcMarkers);
                qMarkers.addToSelect(qcMarkers);
                qMarkers.setConstraint(new BagConstraint(new QueryField(qcMarkers, "name"), ConstraintOp.IN, markerNames));
                Set<GeneticMarker> markers = new HashSet<>();
                Results resultsMarkers = osw.getObjectStore().execute(qMarkers);
                for (Object markerObject : resultsMarkers.asList()) {
                    ResultsRow markerRow = (ResultsRow) markerObject;
                    markers.add((GeneticMarker) markerRow.get(0));
                }
                // store the markers collection for this linkage group
                osw.beginTransaction();
                lg.setMarkers(markers);
                osw.store(lg);
                osw.commitTransaction();
            }
        }
    }
}
