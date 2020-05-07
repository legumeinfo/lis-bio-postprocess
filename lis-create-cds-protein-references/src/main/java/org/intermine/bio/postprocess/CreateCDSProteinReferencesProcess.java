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
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.model.bio.CDS;
import org.intermine.model.bio.Protein;

import org.apache.log4j.Logger;

/**
 * Fill the CDS.protein reference with the protein with the same primaryIdentifier.
 *
 * @author Sam Hokin
 */
public class CreateCDSProteinReferencesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateCDSProteinReferencesProcess.class);

    /**
     * Create a new instance of CreateCDSProteinReferencesProcess
     * @param osw object store writer
-     */
    public CreateCDSProteinReferencesProcess(ObjectStoreWriter osw) {
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

        // query CDSes and Proteins with the same primaryIdentifier
        Query query = new Query();
        query.setDistinct(false);
	// 0
        QueryClass qcCDS = new QueryClass(CDS.class);
        query.addFrom(qcCDS);
        query.addToSelect(qcCDS);
        // 1
        QueryClass qcProtein = new QueryClass(Protein.class);
        query.addFrom(qcProtein);
        query.addToSelect(qcProtein);

	// transcript and CDS identifiers are identical
        query.setConstraint(new SimpleConstraint(new QueryField(qcCDS,"primaryIdentifier"), ConstraintOp.EQUALS, new QueryField(qcProtein,"primaryIdentifier")));
        
        // execute the query
        Results results = osw.getObjectStore().execute(query);
        Iterator<?> iter = results.iterator();

        // begin transaction for storage and run through the records
        osw.beginTransaction();
        while (iter.hasNext()) {
	    ResultsRow<?> rr = (ResultsRow<?>) iter.next();
	    Protein protein = (Protein) rr.get(1);
	    try {
		CDS cds = PostProcessUtil.cloneInterMineObject((CDS) rr.get(0));
		cds.setFieldValue("protein", protein);
		osw.store(cds);
	    } catch (IllegalAccessException e) {
                throw new ObjectStoreException(e);
            }
        }
        osw.commitTransaction();
        osw.close();
    }
        
}
