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
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.Protein;
import org.intermine.model.bio.Transcript;

import org.apache.log4j.Logger;

/**
 * Fill the CDS.gene, CDS.transcript and CDS.protein relations from Transcripts with the same primaryIdentifier.
 *
 * @author Sam Hokin
 */
public class CreateCDSReferencesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateCDSReferencesProcess.class);

    /**
     * Create a new instance of CreateCDSReferencesProcess
     * @param osw object store writer
-     */
    public CreateCDSReferencesProcess(ObjectStoreWriter osw) {
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

        // query Transcripts and Proteins with the same primaryIdentifier
        Query query = new Query();
        query.setDistinct(false);
        
        // 0
        QueryClass qcTranscript = new QueryClass(Transcript.class);
        query.addFrom(qcTranscript);
        query.addToSelect(qcTranscript);

        // 1
        QueryClass qcCDS = new QueryClass(CDS.class);
        query.addFrom(qcCDS);
        query.addToSelect(qcCDS);

        query.setConstraint(new SimpleConstraint(new QueryField(qcCDS,"primaryIdentifier"), ConstraintOp.EQUALS, new QueryField(qcTranscript,"primaryIdentifier")));
        
        // execute the query
        Results results = osw.getObjectStore().execute(query);
        Iterator<?> iter = results.iterator();

        // begin transaction
        osw.beginTransaction();

        while (iter.hasNext()) {
            try {
                ResultsRow<?> rr = (ResultsRow<?>) iter.next();
                // we don't store the Transcript again, no need to clone it
                Transcript transcript = (Transcript) rr.get(0);
                // clone the CDS since we'll update and store it again
                CDS cds = PostProcessUtil.cloneInterMineObject((CDS) rr.get(1));
                // set the CDS.transcript reference
                cds.setFieldValue("transcript", transcript);
                // set the CDS.gene reference if available
                Gene gene = (Gene) transcript.getFieldValue("gene");
                if (gene!=null) {
                    cds.setFieldValue("gene", gene);
                }
                // set the CDS.protein reference if available
                Protein protein = (Protein) transcript.getFieldValue("protein");
                if (protein!=null) {
                    cds.setFieldValue("protein", protein);
                }
                // store the updated CDS
                osw.store(cds);
            } catch (IllegalAccessException ex) {
                throw new ObjectStoreException(ex);
            }
        }
        
        // close transaction
        osw.commitTransaction();

        // close connection
        osw.close();
    }
        
}
