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

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryExpression;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.model.bio.GeneFamily;

import org.apache.log4j.Logger;

/**
 * Store the size of the genes collection per gene family in GeneFamily.size.
 *
 * @author Sam Hokin
 */
public class PopulateGeneFamilySizeProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateGeneFamilySizeProcess.class);

    /**
     * Populate a new instance of PopulateCDSGeneProcess
     * @param osw object store writer
-     */
    public PopulateGeneFamilySizeProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        Query q = new Query();
	// 0
        QueryClass qcGeneFamily = new QueryClass(GeneFamily.class);
        q.addFrom(qcGeneFamily);
        q.addToSelect(qcGeneFamily);
	// // 1
        // QueryClass qcGene = new QueryClass(Gene.class);
        // q.addFrom(qcGene);
        // q.addToSelect(qcGene);
        // // CDS.primaryIdentifier LIKE Gene.primaryIdentifier.%
	// QueryField cdsPrimaryIdentifier = new QueryField(qcCDS, "primaryIdentifier");
	// QueryField genePrimaryIdentifier = new QueryField(qcGene, "primaryIdentifier");
	// QueryValue dotPercent = new QueryValue(".%");
	// QueryExpression genePrimaryIdentifierMatch = new QueryExpression(genePrimaryIdentifier, QueryExpression.CONCAT, dotPercent);
	// SimpleConstraint sc = new SimpleConstraint(cdsPrimaryIdentifier, ConstraintOp.MATCHES, genePrimaryIdentifierMatch);
	// q.setConstraint(sc);
	
        // execute the query
        Results results = osw.getObjectStore().execute(q);

        // int batchSize = 1000;
        // boolean optimise = true;
        // boolean explain = false; // crashes if true
        // boolean prefetch = true;
        // Results results = osw.getObjectStore().execute(q, batchSize, optimise, explain, prefetch);

        // begin transaction for storing and run through the records
	osw.beginTransaction();
	for (Object resultObject : results.asList()) {
	    ResultsRow row = (ResultsRow) resultObject;
	    try {
		GeneFamily gf = PostProcessUtil.cloneInterMineObject((GeneFamily) row.get(0));
                gf.setFieldValue("size", gf.getGenes().size());
		osw.store(gf);
	    } catch (IllegalAccessException e) {
		throw new ObjectStoreException(e);
	    }
	}
	osw.commitTransaction();
        osw.close();
    }
        
}
