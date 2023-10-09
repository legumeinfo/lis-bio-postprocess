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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.intermine.bio.util.PostProcessUtil;

import org.intermine.metadata.ConstraintOp;

import org.intermine.model.bio.DataSet;
import org.intermine.model.bio.DataSource;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.Pathway;

import org.intermine.postprocess.PostProcessor;

import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryExpression;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

/**
 * Populate the Pathways class and Gene.pathways collections from Plant Reactome entries, if present.
 * IMPORTANT: LIS gene names are munged into Plant Reactome gene names in a method that MUST BE UPDATED REGULARLY!
 * NOTE: update PLANT_REACTOME_VERSION when the gene file is updated!
 *
 * @author Sam Hokin
 */
public class CreatePathwaysProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreatePathwaysProcess.class);
    public static final String PLANT_REACTOME_GENE_FILE = "gene_ids_by_pathway_and_species.tab";
    public static final String PLANT_REACTOME_VERSION = "Version 23 (Gramene 67)";

    /**
     * Populate a new instance of CreatePathwaysProcess
     *
     * @param osw object store writer
     */
    public CreatePathwaysProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException, IOException, IllegalAccessException {
        // delete existing Pathways
        Query qPathway = new Query();
        QueryClass qcPathway = new QueryClass(Pathway.class);
        qPathway.addFrom(qcPathway);
        qPathway.addToSelect(qcPathway);
        List<Pathway> oldPathways = new ArrayList<>();
        Results pathwayResults = osw.getObjectStore().execute(qPathway);
        for (Object o : pathwayResults.asList()) {
            ResultsRow row = (ResultsRow) o;
            Pathway pathway = (Pathway) row.get(0);
            oldPathways.add(pathway);
        }
        osw.beginTransaction();
        for (Pathway pathway : oldPathways) {
            osw.delete(pathway);
        }
        osw.commitTransaction();
        System.out.println("### Deleted " + oldPathways.size() + " Pathway objects.");
        LOG.info("### Deleted " + oldPathways.size() + " Pathway objects.");

        // use existing Gramene DataSource if it exists (could be stuff other than Plant Reactome)
        Query qDataSource = new Query();
        QueryClass qcDataSource = new QueryClass(DataSource.class);
        qDataSource.addFrom(qcDataSource);
        qDataSource.addToSelect(qcDataSource);
        QueryField dataSourceNameField = new QueryField(qcDataSource, "name");
        QueryValue dataSourceNameValue = new QueryValue("Gramene");
        qDataSource.setConstraint(new SimpleConstraint(dataSourceNameValue, ConstraintOp.EQUALS, dataSourceNameField));
        // execute the query, populating DataSource if present
        DataSource dataSource = null;
        Results dataStoreResults = osw.getObjectStore().execute(qDataSource);
	for (Object resultObject : dataStoreResults.asList()) {
	    ResultsRow row = (ResultsRow) resultObject;
            dataSource = (DataSource) row.get(0);
	}
        // create and store the Gramene DataSource if not already present
        if (dataSource == null) {
            dataSource = (DataSource) DynamicUtil.createObject(Collections.singleton(DataSource.class));
            dataSource.setName("Gramene");
            dataSource.setUrl("https://www.gramene.org/");
            dataSource.setDescription("Comparative plant genomics for crops and model organisms");
            osw.beginTransaction();
            osw.store(dataSource);
            osw.commitTransaction();
            System.out.println("### Stored new Gramene DataSource.");
            LOG.info("### Stored new Gramene DataSource.");
        } else {
            System.out.println("### Using existing Gramene DataSource.");
            LOG.info("### Using existing Gramene DataSource.");
        }
        
        // delete the existing Plant Reactome DataSet, if present, since version may have changed
        Query qDataSet = new Query();
        QueryClass qcDataSet = new QueryClass(DataSet.class);
        qDataSet.addFrom(qcDataSet);
        qDataSet.addToSelect(qcDataSet);
        QueryField dataSetNameField = new QueryField(qcDataSet, "name");
        QueryValue dataSetNameValue = new QueryValue("Plant Reactome");
        qDataSet.setConstraint(new SimpleConstraint(dataSetNameValue, ConstraintOp.EQUALS, dataSetNameField));
        List<DataSet> oldDataSets = new ArrayList<>();
        Results dataSetResults = osw.getObjectStore().execute(qDataSet);
        for (Object o : dataSetResults.asList()) {
            ResultsRow row = (ResultsRow) o;
            DataSet dataSet = (DataSet) row.get(0);
            oldDataSets.add(dataSet);
        }
        osw.beginTransaction();
        for (DataSet dataSet : oldDataSets) {
            osw.delete(dataSet);
        }
        osw.commitTransaction();
        System.out.println("### Deleted " + oldDataSets.size() + " Plant Reactome DataSet objects.");
        LOG.info("### Deleted " + oldDataSets.size() + " DataSet objects.");
        
        // create and store the new Plant Reactome DataSet
        DataSet dataSet = (DataSet) DynamicUtil.createObject(Collections.singleton(DataSet.class));
        dataSet.setName("Plant Reactome");
        dataSet.setUrl("https://plantreactome.gramene.org/");
        dataSet.setDescription("PLANT REACTOME is an open-source, open access, manually curated and peer-reviewed pathway database. " +
                                  "Pathway annotations are authored by expert biologists, in collaboration with the Reactome editorial " +
                                  "staff and cross-referenced to many bioinformatics databases. " +
                                  "These include project databases like Gramene, Ensembl, UniProt, ChEBI small molecule databases, " +
                                  "PubMed, and Gene Ontology.");
        dataSet.setLicence("CC BY 3.0");
        dataSet.setVersion(PLANT_REACTOME_VERSION);
        dataSet.setDataSource(dataSource);
        osw.beginTransaction();
        osw.store(dataSet);
        osw.commitTransaction();
        System.out.println("### Stored new Plant Reactome DataSet.");
        LOG.info("### Stored new Plant Reactome DataSet.");
        
        // load the Plant Reactome gene file into maps of identifier/name lists keyed by gene
        // 0=identifier         1=name                  2=species               3=gene
        // R-PVU-1119430.1	Chorismate biosynthesis	Phaseolus vulgaris	PHAVU_002G106700g
        Map<String,String> pathwayIdentifierName = new HashMap<>();             // pathway identifier to name
        Map<String,List<String>> geneNamePathwayIdentifiers = new HashMap<>();  // gene name to list of pathway identifiers
        Map<String,List<String>> geneNamePathwayNames = new HashMap<>();        // gene name to list of pathway names
        InputStream is = getClass().getClassLoader().getResourceAsStream(PLANT_REACTOME_GENE_FILE);
        if (is == null) throw new RuntimeException("Could not load Plant Reactome gene file " + PLANT_REACTOME_GENE_FILE);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("\t");
            if (fields.length == 4) {
                String pathwayIdentifier = fields[0];
                String pathwayName = fields[1];
                String species = fields[2];
                String geneName = fields[3];
                pathwayIdentifierName.put(pathwayIdentifier, pathwayName);
                if (geneNamePathwayIdentifiers.containsKey(geneName)) {
                    geneNamePathwayIdentifiers.get(geneName).add(pathwayIdentifier);
                    geneNamePathwayNames.get(geneName).add(pathwayName);
                } else {
                    List<String> pathwayIdentifiers = new ArrayList<>();
                    List<String> pathwayNames = new ArrayList<>();
                    pathwayIdentifiers.add(pathwayIdentifier);
                    pathwayNames.add(pathwayName);
                    geneNamePathwayIdentifiers.put(geneName, pathwayIdentifiers);
                    geneNamePathwayNames.put(geneName, pathwayNames);
                }
            }
        }
        
        // query all genes in the mine
        Query qGene = new Query();
        QueryClass qcGene = new QueryClass(Gene.class);
        qGene.addFrom(qcGene);
        qGene.addToSelect(qcGene);
        // execute the query
        Results geneResults = osw.getObjectStore().execute(qGene);
        // store Genes in a Map keyed by id IF they have non-null ensemblName
        Map<Integer,Gene> genes = new HashMap<>();
	for (Object resultObject : geneResults.asList()) {
	    ResultsRow row = (ResultsRow) resultObject;
            Gene gene = (Gene) row.get(0);
            if (gene.getEnsemblName() != null) {
                genes.put(gene.getId(), gene);
            }
	}

        // run through the genes and populate gene-pathway maps
        Map<Gene,List<String>> genePathwayIdentifiers  = new HashMap<>();
        Map<Gene,List<String>> genePathwayNames  = new HashMap<>();
        for (Gene gene : genes.values()) {
            List<String> pathwayIdentifiers = geneNamePathwayIdentifiers.get(gene.getEnsemblName());
            List<String> pathwayNames = geneNamePathwayNames.get(gene.getEnsemblName());
            if (pathwayIdentifiers != null) {
                genePathwayIdentifiers.put(gene, pathwayIdentifiers);
                genePathwayNames.put(gene, pathwayNames);
            }
        }

        // load a map of pathway identifier to pathway name to only store those assocated with our genes
        Map<String,String> storedPathwayIdentifierName = new HashMap<>();
        for (Gene gene : genePathwayIdentifiers.keySet()) {
            List<String> identifiers = genePathwayIdentifiers.get(gene);
            List<String> names = genePathwayNames.get(gene);
            for (int i = 0; i < identifiers.size(); i++) {
                storedPathwayIdentifierName.put(identifiers.get(i), names.get(i));
            }
        }
        
        // store the pathways associated with genes in the mine
        // and save the Pathway objects in a map for gene updates
        Map<String,Pathway> pathways = new HashMap<>();
	osw.beginTransaction();
        for (String identifier : storedPathwayIdentifierName.keySet()) {
            String name = storedPathwayIdentifierName.get(identifier);
            Pathway pathway = (Pathway) DynamicUtil.createObject(Collections.singleton(Pathway.class));
            pathway.setPrimaryIdentifier(identifier);
            pathway.setName(name);
            pathway.addDataSets(dataSet);
            osw.store(pathway);
            pathways.put(identifier, pathway);
        }
        osw.commitTransaction();
        System.out.println("### Stored " + storedPathwayIdentifierName.size() + " Pathway objects.");
        LOG.info("### Stored " + storedPathwayIdentifierName.size() + " Pathway objects.");

        // update Gene.pathways collection
	osw.beginTransaction();
        for (Gene g : genePathwayIdentifiers.keySet()) {
            Gene gene = PostProcessUtil.cloneInterMineObject(g);
            Set<Pathway> genePathways = new HashSet<>();
            for (String identifier : genePathwayIdentifiers.get(g)) {
                genePathways.add(pathways.get(identifier));
            }
            // NOTE: not sure why I have to use setFieldValue rather than addPathways or setPathways
            // when using cloneInterMineObject.
            gene.setFieldValue("pathways", genePathways);
            osw.store(gene);
        }
        osw.commitTransaction();
        System.out.println("### Updated " + genePathwayIdentifiers.size() + " Gene objects.");
        LOG.info("### Updated " + genePathwayIdentifiers.size() + " Gene objects.");
            
        // close the ObjectStoreWriter
        osw.close();
    }
    
}
