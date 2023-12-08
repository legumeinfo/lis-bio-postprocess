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
import org.intermine.model.bio.OntologyTerm;
import org.intermine.model.bio.Protein;
import org.intermine.model.bio.Transcript;

import org.apache.log4j.Logger;

/**
 * Remove orphaned objects:
 *
 *   - Gene with empty proteins collection
 *   - Protein with empty genes collection OR null transcript reference
 *   - Transcript with null protein reference or null chromosome/supercontig reference
 *   - OntologyTerm with null ontology reference
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

        Set<Gene> orphanGenes = new HashSet<>();
        // find Gene objects with empty proteins collection
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

        Set<OntologyTerm> orphanOntologyTerms = new HashSet<>();
        // find OntologyTerm objects with no ontology reference
        Query qOntologyTerm = new Query();
        QueryClass qcOntologyTerm = new QueryClass(OntologyTerm.class);
        qOntologyTerm.addFrom(qcOntologyTerm);
        qOntologyTerm.addToSelect(qcOntologyTerm);
        qOntologyTerm.setConstraint(new ContainsConstraint(new QueryObjectReference(qcOntologyTerm, "ontology"), ConstraintOp.IS_NULL));
        Results ontologyTermResults = osw.getObjectStore().execute(qOntologyTerm);
        for (Object obj : ontologyTermResults.asList()) {
            ResultsRow row = (ResultsRow) obj;
            OntologyTerm ontologyTerm = (OntologyTerm) row.get(0);
            orphanOntologyTerms.add(ontologyTerm);
        }

        Set<Protein> orphanProteins = new HashSet<>();
        // find Protein objects with empty genes collection OR no transcript reference
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

        Set<Transcript> orphanTranscripts = new HashSet<>();
        // Transcript 1. find Transcript objects with no protein reference
        Query qTranscript1 = new Query();
        QueryClass qcTranscript1 = new QueryClass(Transcript.class);
        qTranscript1.addFrom(qcTranscript1);
        qTranscript1.addToSelect(qcTranscript1);
        qTranscript1.setConstraint(new ContainsConstraint(new QueryObjectReference(qcTranscript1, "protein"), ConstraintOp.IS_NULL));
        Results transcriptResults1 = osw.getObjectStore().execute(qTranscript1);
        for (Object obj : transcriptResults1.asList()) {
            ResultsRow row = (ResultsRow) obj;
            Transcript transcript = (Transcript) row.get(0);
            orphanTranscripts.add(transcript);
        }
        // Transcript 2. find Transcript objects with no chromosome reference AND no supercontig reference
        Query qTranscript2 = new Query();
        QueryClass qcTranscript2 = new QueryClass(Transcript.class);
        qTranscript2.addFrom(qcTranscript2);
        qTranscript2.addToSelect(qcTranscript2);
        ConstraintSet transcriptConstraints2 = new ConstraintSet(ConstraintOp.AND);
        transcriptConstraints2.addConstraint(new ContainsConstraint(new QueryObjectReference(qcTranscript2, "chromosome"), ConstraintOp.IS_NULL));
        transcriptConstraints2.addConstraint(new ContainsConstraint(new QueryObjectReference(qcTranscript2, "supercontig"), ConstraintOp.IS_NULL));
        qTranscript2.setConstraint(transcriptConstraints2);
        Results transcriptResults2 = osw.getObjectStore().execute(qTranscript2);
        for (Object obj : transcriptResults2.asList()) {
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

        // remove orphaned ontology terms
        osw.beginTransaction();
        for (OntologyTerm ontologyTerm : orphanOntologyTerms) {
            osw.delete(ontologyTerm);
        }
        osw.commitTransaction();
        LOG.info("Removed " + orphanOntologyTerms.size() + " OntologyTerm objects with no ontology reference.");

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
        LOG.info("Removed " + orphanTranscripts.size() + " Transcript objects with no protein reference or which lack both chromosome and supercontig reference.");
    }
}
