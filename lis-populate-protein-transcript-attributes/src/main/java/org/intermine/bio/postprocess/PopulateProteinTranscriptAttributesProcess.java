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
import org.intermine.objectstore.query.MultipleInBagConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.intermine.model.bio.Gene;
import org.intermine.model.bio.PanGeneSet;
import org.intermine.model.bio.Protein;
import org.intermine.model.bio.Transcript;

import org.apache.log4j.Logger;

/**
 * Populate Protein and Transcript name, description from a Gene associated with the Protein.
 *
 * @author Sam Hokin
 */
public class PopulateProteinTranscriptAttributesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateProteinTranscriptAttributesProcess.class);

    /**
     * @param osw object store writer
     */
    public PopulateProteinTranscriptAttributesProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        // query Proteins, store those that do NOT have name OR description
        List<Protein> proteins = new ArrayList<>();
        Query qProtein = new Query();
        QueryClass qcProtein = new QueryClass(Protein.class);
        qProtein.addFrom(qcProtein);
        qProtein.addToSelect(qcProtein);
        Results results = osw.getObjectStore().execute(qProtein);
        for (Object obj : results.asList()) {
            ResultsRow row = (ResultsRow) obj;
            Protein protein = (Protein) row.get(0);
            if (protein.getDescription() == null || protein.getName() == null) {
                proteins.add(protein);
            }
        }
        LOG.info("Found " + proteins.size() + " proteins that lack name or description.");

        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // spin through the Proteins, getting their associated transcript and gene(s), grabbing the Gene.name and Gene.description
        Map<Protein,String> proteinNames = new ConcurrentHashMap<>();
        Map<Protein,String> proteinDescriptions = new ConcurrentHashMap<>();
        Map<Transcript,String> transcriptNames = new ConcurrentHashMap<>();
        Map<Transcript,String> transcriptDescriptions = new ConcurrentHashMap<>();
        proteins.parallelStream().forEach(protein -> {
                Transcript transcript = protein.getTranscript();
                Set<Gene> genes = protein.getGenes();
                String name = null;
                String description = null;
                for (Gene gene : genes) {
                    if (gene.getName() != null) name = gene.getName();
                    if (gene.getDescription() != null) description = gene.getDescription();
                }
                if (name != null) {
                    proteinNames.put(protein, name);
                    transcriptNames.put(transcript, name);
                }
                if (description != null) {
                    proteinDescriptions.put(protein, description);
                    transcriptDescriptions.put(transcript, description);
                }
            });
        Set<Protein> proteinsToUpdate = proteinNames.keySet();
        proteinsToUpdate.addAll(proteinDescriptions.keySet());
        Set<Transcript> transcriptsToUpdate = transcriptNames.keySet();
        transcriptsToUpdate.addAll(transcriptDescriptions.keySet());

        // update and store Proteins
        LOG.info("Updating " + proteinsToUpdate.size() + " proteins...");
        osw.beginTransaction();
        for (Protein protein : proteinsToUpdate) {
            try {
                Protein clone = PostProcessUtil.cloneInterMineObject(protein);
                if (proteinNames.containsKey(protein)) clone.setName(proteinNames.get(protein));
                if (proteinDescriptions.containsKey(protein)) clone.setDescription(proteinNames.get(protein));
                osw.store(clone);
            } catch (IllegalAccessException ex) {
                System.err.println(ex);
                throw new RuntimeException(ex);
            }
        }
        osw.commitTransaction();
        
        // update and store Transcripts
        LOG.info("Updating " + transcriptsToUpdate.size() + " transcripts...");
        osw.beginTransaction();
        for (Transcript transcript : transcriptsToUpdate) {
            try {
                Transcript clone = PostProcessUtil.cloneInterMineObject(transcript);
                if (transcriptNames.containsKey(transcript)) clone.setName(transcriptNames.get(transcript));
                if (transcriptDescriptions.containsKey(transcript)) clone.setDescription(transcriptNames.get(transcript));
                osw.store(clone);
            } catch (IllegalAccessException ex) {
                System.err.println(ex);
                throw new RuntimeException(ex);
            }
        }
        osw.commitTransaction();
    }
}
