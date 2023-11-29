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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.MultipleInBagConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.Constraint;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.Protein;
import org.intermine.model.bio.Transcript;

import org.apache.log4j.Logger;

/**
 * Remove orphaned objects.
 *
 * @author Sam Hokin
 */
public class RemoveOrphansProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(RemoveOrphansProcess.class);

    /**
     * @param osw object store writer
     */
    public RemoveOrphansProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        // find Gene objects with empty proteins collection
        Set<Gene> orphanGenes = new HashSet<>();
        Query qGene = new Query();
        QueryClass qcGene = new QueryClass(Gene.class);
        qGene.addFrom(qcGene);
        qGene.addToSelect(qcGene);
        qGene.setConstraint(new ContainsConstraint(new QueryCollectionReference(qcGene, "proteins"), ConstraintOp.IS_EMPTY));
        Results geneResults = osw.getObjectStore().execute(qGene);
        for (Object obj : geneResults.asList()) {
            ResultsRow row = (ResultsRow) obj;
            Gene gene = (Gene) row.get(0);
            orphanGenes.add(gene);
        }

        // find Protein objects with empty genes collection OR no transcript reference
        Set<Protein> orphanProteins = new HashSet<>();
        Query qProtein = new Query();
        QueryClass qcProtein = new QueryClass(Protein.class);
        qProtein.addFrom(qcProtein);
        qProtein.addToSelect(qcProtein);
        ConstraintSet proteinConstraints = new ConstraintSet(ConstraintOp.OR);
        proteinConstraints.addConstraint(new ContainsConstraint(new QueryCollectionReference(qcProtein, "genes"), ConstraintOp.IS_EMPTY));
        proteinConstraints.addConstraint(new ContainsConstraint(new QueryObjectReference(qcProtein, "transcript"), ConstraintOp.IS_NULL));
        qProtein.setConstraint(proteinConstraints);
        Results proteinResults = osw.getObjectStore().execute(qProtein);
        for (Object obj : proteinResults.asList()) {
            ResultsRow row = (ResultsRow) obj;
            Protein protein = (Protein) row.get(0);
            orphanProteins.add(protein);
        }

        // find orphan Transcript objects
        Set<Transcript> orphanTranscripts = new HashSet<>();
        Query qTranscript = new Query();
        QueryClass qcTranscript = new QueryClass(Transcript.class);
        qTranscript.addFrom(qcTranscript);
        qTranscript.addToSelect(qcTranscript);
        qTranscript.setConstraint(new ContainsConstraint(new QueryObjectReference(qcTranscript, "protein"), ConstraintOp.IS_NULL));
        Results transcriptResults = osw.getObjectStore().execute(qTranscript);
        for (Object obj : transcriptResults.asList()) {
            ResultsRow row = (ResultsRow) obj;
            Transcript transcript = (Transcript) row.get(0);
            orphanTranscripts.add(transcript);
        }

        // remove orphaned genes
        osw.beginTransaction();
        for (Gene gene : orphanGenes) {
            osw.delete(gene);
        }
        osw.commitTransaction();
        LOG.info("Removed " + orphanGenes.size() + " Gene objects with empty proteins collection.");

        // remove orphaned proteins
        osw.beginTransaction();
        for (Protein protein : orphanProteins) {
            osw.delete(protein);
        }
        osw.commitTransaction();
        LOG.info("Removed " + orphanProteins.size() + " Protein objects with empty genes collection OR no transcript reference.");

        // remove orphaned transcripts
        osw.beginTransaction();
        for (Transcript transcript : orphanTranscripts) {
            osw.delete(transcript);
        }
        osw.commitTransaction();
        LOG.info("Removed " + orphanTranscripts.size() + " Transcript objects with no protein reference.");
    }
}
