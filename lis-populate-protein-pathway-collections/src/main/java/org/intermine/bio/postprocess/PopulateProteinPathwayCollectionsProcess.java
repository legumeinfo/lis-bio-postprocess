package org.intermine.bio.postprocess;

/*
 * Copyright (C) 2002-2020 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.intermine.bio.util.Constants;
import org.intermine.bio.util.PostProcessUtil;
import org.intermine.metadata.ClassDescriptor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.metadata.Model;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.Pathway;
import org.intermine.model.bio.Protein;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
import org.intermine.objectstore.intermine.ObjectStoreWriterInterMineImpl;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.postprocess.PostProcessor;
import org.intermine.sql.DatabaseUtil;
import org.intermine.util.DynamicUtil;


/**
 * Copy over Gene.pathways to Protein.pathways
 *
 * @author Julie Sullivan
 * @author Sam Hokin
 */
public class PopulateProteinPathwayCollectionsProcess extends PostProcessor
{
    private static final Logger LOG = Logger.getLogger(PopulateProteinPathwayCollectionsProcess.class);
    private Model model;

    /**
     * Constructor
     *
     * @param osw object store writer
     */
    public PopulateProteinPathwayCollectionsProcess(ObjectStoreWriter osw) {
        super(osw);
        model = Model.getInstanceByName("genomic");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void postProcess() {
        try {
            copyGenePathways();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Copy Gene.pathways to Protein.pathways
     */
    private void copyGenePathways() throws ObjectStoreException, IllegalAccessException, SQLException {
        Results results = findGenePathways(osw.getObjectStore());
        int count = 0;
        Protein lastProtein = null;
        Set<Pathway> newCollection = new HashSet<Pathway>();

        osw.beginTransaction();

        Iterator<?> resIter = results.iterator();
        while (resIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) resIter.next();
            Protein thisProtein = (Protein) rr.get(0);
            Pathway pathway = (Pathway) rr.get(1);

            if (lastProtein == null || !thisProtein.getId().equals(lastProtein.getId())) {
                if (lastProtein != null) {
                    // clone so we don't change the ObjectStore cache
                    Protein tempProtein = PostProcessUtil.cloneInterMineObject(lastProtein);
                    tempProtein.setFieldValue("pathways", newCollection);
                    osw.store(tempProtein);
                    count++;
                }
                newCollection = new HashSet<Pathway>();
            }
            newCollection.add(pathway);
            lastProtein = thisProtein;
        }

        if (lastProtein != null) {
            // clone so we don't change the ObjectStore cache
            Protein tempProtein = PostProcessUtil.cloneInterMineObject(lastProtein);
            tempProtein.setFieldValue("pathways", newCollection);
            osw.store(tempProtein);
            count++;
        }
        LOG.info("Created " + count + " Protein.pathways collections");
        osw.commitTransaction();

        // now ANALYSE tables relating to class that has been altered - may be rows added
        // to indirection tables
        if (osw instanceof ObjectStoreWriterInterMineImpl) {
            ClassDescriptor cld = model.getClassDescriptorByName(Protein.class.getName());
            DatabaseUtil.analyse(((ObjectStoreWriterInterMineImpl) osw).getDatabase(), cld, false);
        }
    }

    /**
     * Run a query that returns all genes, proteins, and associated pathways.
     *
     * @param os the objectstore
     * @return the Results object
     * @throws ObjectStoreException if there is an error while reading from the ObjectStore
     */
    protected static Results findGenePathways(ObjectStore os) throws ObjectStoreException {
        Query q = new Query();
        QueryClass qcGene = new QueryClass(Gene.class);
        QueryClass qcProtein = new QueryClass(Protein.class);
        QueryClass qcPathway = new QueryClass(Pathway.class);

        q.addFrom(qcProtein);
        q.addFrom(qcGene);
        q.addFrom(qcPathway);

        q.addToSelect(qcProtein);
        q.addToSelect(qcPathway);

        ConstraintSet cs = new ConstraintSet(ConstraintOp.AND);

        // Gene.proteins
        QueryCollectionReference c1 = new QueryCollectionReference(qcGene, "proteins");
        cs.addConstraint(new ContainsConstraint(c1, ConstraintOp.CONTAINS, qcProtein));

        // Gene.pathways
        QueryCollectionReference c2 = new QueryCollectionReference(qcGene, "pathways");
        cs.addConstraint(new ContainsConstraint(c2, ConstraintOp.CONTAINS, qcPathway));

        q.setConstraint(cs);

        ObjectStoreInterMineImpl osimi = (ObjectStoreInterMineImpl) os;
        osimi.precompute(q, Constants.PRECOMPUTE_CATEGORY);
        Results res = os.execute(q);

        return res;
    }
}
