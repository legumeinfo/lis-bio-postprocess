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

import org.intermine.model.bio.CDS;
import org.intermine.model.bio.Gene;

import org.apache.log4j.Logger;

/**
 * Fill the CDS.gene references by matching Gene.primaryIdentifier with CDS.primaryIdentifier.*
 *
 * @author Sam Hokin
 */
public class PopulateCDSGeneReferencesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateCDSGeneReferencesProcess.class);

    /**
     * Populate a new instance of PopulateCDSGeneReferencesProcess
     * @param osw object store writer
-     */
    public PopulateCDSGeneReferencesProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        Query q = new Query();
	// 0
        QueryClass qcCDS = new QueryClass(CDS.class);
        q.addFrom(qcCDS);
        q.addToSelect(qcCDS);
	// 1
        QueryClass qcGene = new QueryClass(Gene.class);
        q.addFrom(qcGene);
        q.addToSelect(qcGene);
	// cds.primaryIdentifier = gene.primaryIdentifier.1 (primary CDS)
	QueryField cdsPrimaryIdentifier = new QueryField(qcCDS, "primaryIdentifier");
	QueryField genePrimaryIdentifier = new QueryField(qcGene, "primaryIdentifier");
	SimpleConstraint sc = new SimpleConstraint(cdsPrimaryIdentifier, ConstraintOp.CONTAINS, genePrimaryIdentifier);
	q.setConstraint(sc);
	
        // execute the query
        Results results = osw.getObjectStore().execute(q);
	List<Object> resultObjects = results.asList();
	
        // begin transaction for storing and run through the matches
	osw.beginTransaction();
	for (Object resultObject : resultObjects) {
	    ResultsRow row = (ResultsRow) resultObject;
	    try {
		CDS cds = PostProcessUtil.cloneInterMineObject((CDS) row.get(0));
		Gene gene = (Gene) row.get(1);
		cds.setFieldValue("gene", gene);
		osw.store(cds);
	    } catch (IllegalAccessException e) {
		throw new ObjectStoreException(e);
	    }
	}
	osw.commitTransaction();
        osw.close();
    }
        
}
