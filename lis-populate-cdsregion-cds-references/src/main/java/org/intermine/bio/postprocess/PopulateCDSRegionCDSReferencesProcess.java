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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.model.bio.CDS;
import org.intermine.model.bio.CDSRegion;

import org.apache.log4j.Logger;

/**
 * Fill the CDS.transcript reference with the transcript with the same primaryIdentifier.
 *
 * @author Sam Hokin
 */
public class PopulateCDSRegionCDSReferencesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateCDSRegionCDSReferencesProcess.class);

    /**
     * Populate a new instance of PopulateCDSRegionCDSReferencesProcess
     * @param osw object store writer
-     */
    public PopulateCDSRegionCDSReferencesProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     *
     * Main method
     *
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void postProcess() throws ObjectStoreException {

        // query CDSes and CDSRegions where CDSRegion.primaryIdentifier starts with CDS.primaryIdentifier
        Query query = new Query();
        query.setDistinct(false);

        // 0
        QueryClass qcCDS = new QueryClass(CDS.class);
        query.addFrom(qcCDS);
        query.addToSelect(qcCDS);
	// 1
        QueryClass qcCDSRegion = new QueryClass(CDSRegion.class);
        query.addFrom(qcCDSRegion);
        query.addToSelect(qcCDSRegion);

	// CDSRegion identifier should contain CDS identifier, restrict to CDSRegion objects that haven't yet been set
        ConstraintSet constraints = new ConstraintSet(ConstraintOp.AND);
        constraints.addConstraint(new SimpleConstraint(new QueryField(qcCDSRegion,"primaryIdentifier"), ConstraintOp.CONTAINS, new QueryField(qcCDS,"primaryIdentifier")));
        constraints.addConstraint(new SimpleConstraint(new QueryField(qcCDSRegion,"CDS"), ConstraintOp.IS_NULL));
        query.setConstraint(constraints);

        // execute the query
        Results results = osw.getObjectStore().execute(query);
        Iterator<?> iter = results.iterator();

        // begin transaction for storage and run through the records
        osw.beginTransaction();
        while (iter.hasNext()) {
	    ResultsRow<?> rr = (ResultsRow<?>) iter.next();
            CDS cds = (CDS) rr.get(0);
            CDSRegion cdsRegion = (CDSRegion) rr.get(1);
            try {
                CDSRegion tempCDSRegion = PostProcessUtil.cloneInterMineObject(cdsRegion);
                tempCDSRegion.setFieldValue("CDS", cds);
                osw.store(tempCDSRegion);
            } catch (IllegalAccessException e) {
                throw new ObjectStoreException(e);
            }
        }
        osw.commitTransaction();
        osw.close();
    }
}
