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
        Map<Integer,Protein> proteins = new HashMap<>();
        Query qProtein = new Query();
        QueryClass qcProtein = new QueryClass(Protein.class);
        qProtein.addFrom(qcProtein);
        qProtein.addToSelect(qcProtein);
        Results results = osw.getObjectStore().execute(qProtein);
        for (Object obj : results.asList()) {
            ResultsRow row = (ResultsRow) obj;
            Protein protein = (Protein) row.get(0);
            if (protein.getDescription() == null || protein.getName() == null) {
                proteins.put(protein.getId(), protein);
            }
        }
        LOG.info("Found " + proteins.size() + " proteins that lack name or description.");

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        // spin through the Proteins, getting their associated gene(s), storing the Gene.name and Gene.description
        Map<Integer,String> names = new ConcurrentHashMap<>();
        Map<Integer,String> descriptions = new ConcurrentHashMap<>();
        proteins.keySet().parallelStream().forEach(id -> {
                Protein protein = proteins.get(id);
                Set<Gene> genes = protein.getGenes();
                String name = null;
                String description = null;
                for (Gene gene : genes) {
                    if (gene.getName() != null) name = gene.getName();
                    if (gene.getDescription() != null) description = gene.getDescription();
                }
                if (name != null) {
                    names.put(id, name);
                }
                if (description != null) {
                    descriptions.put(id, description);
                }
            });
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////

        // only update the proteins (and transcripts) that received names and/or descriptions
        Set<Integer> proteinsToUpdate = new HashSet<>();
        proteinsToUpdate.addAll(names.keySet());
        proteinsToUpdate.addAll(descriptions.keySet());

        // update and store proteins and their transcripts
        LOG.info("Updating " + proteinsToUpdate.size() + " proteins and transcripts...");
        osw.beginTransaction();
        for (int id : proteinsToUpdate) {
            Protein protein = proteins.get(id);
            Transcript transcript = protein.getTranscript();
            try {
                Protein pClone = PostProcessUtil.cloneInterMineObject(protein);
                Transcript tClone = PostProcessUtil.cloneInterMineObject(transcript);
                if (names.containsKey(id)) {
                    pClone.setName(names.get(id));
                    tClone.setName(names.get(id));
                }
                if (descriptions.containsKey(id)) {
                    pClone.setDescription(names.get(id));
                    tClone.setDescription(names.get(id));
                }
                osw.store(pClone);
                osw.store(tClone);
            } catch (IllegalAccessException ex) {
                System.err.println(ex);
                throw new RuntimeException(ex);
            }
        }
        osw.commitTransaction();
    }
}
