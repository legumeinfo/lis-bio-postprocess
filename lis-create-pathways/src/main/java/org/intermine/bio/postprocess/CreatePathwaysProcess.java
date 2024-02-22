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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.intermine.bio.util.PostProcessUtil;

import org.intermine.metadata.ConstraintOp;

import org.intermine.model.bio.DataSet;
import org.intermine.model.bio.DataSource;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.Pathway;
import org.intermine.model.bio.Publication;

import org.intermine.postprocess.PostProcessor;

import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

/**
 * Populate the Pathways class and Gene.pathways collections from Plant Reactome entries, if present.
 * IMPORTANT: LIS gene names are munged into Plant Reactome gene names in a method that MUST BE UPDATED REGULARLY!
 *
 * NOTE: update PLANT_REACTOME_VERSION HERE when the gene file is updated!
 *
 * @author Sam Hokin
 */
public class CreatePathwaysProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreatePathwaysProcess.class);

    // UPDATE WHEN THE GENE FILE IS UPDATED!
    public static final String PLANT_REACTOME_VERSION = "Version 23 (Gramene 67)";
    //
    public static final String PLANT_REACTOME_GENE_FILE = "gene_ids_by_pathway_and_species.tab";
    public static final String PLANT_REACTOME_NAME = "Plant Reactome";
    public static final String PLANT_REACTOME_URL = "https://plantreactome.gramene.org/";
    public static final String PLANT_REACTOME_LICENCE = "CC BY 3.0";
    public static final String PLANT_REACTOME_DESCRIPTION = "PLANT REACTOME is an open-source, open access, manually curated and peer-reviewed pathway database. " +
        "Pathway annotations are authored by expert biologists, in collaboration with the Reactome editorial " +
        "staff and cross-referenced to many bioinformatics databases. " +
        "These include project databases like Gramene, Ensembl, UniProt, ChEBI small molecule databases, " +
        "PubMed, and Gene Ontology.";

    public static final String PLANT_REACTOME_PUB_TITLE = "Plant Reactome: a knowledgebase and resource for comparative pathway analysis.";
    public static final String PLANT_REACTOME_PUB_FIRST_AUTHOR = "Naithani S";
    public static final int PLANT_REACTOME_PUB_YEAR = 2020;
    public static final String PLANT_REACTOME_PUB_JOURNAL = "Nucleic Acids Research";
    public static final String PLANT_REACTOME_PUB_DOI = "10.1093/nar/gkz996";
    public static final String PLANT_REACTOME_PUB_AUTHORS = "Naithani S,  Gupta P, Preece J, D’Eustachio P,  Elser J, Garg P, Dikeman DA, " +
        "Kiff J, Cook J, Olson A,  Wei S, Tello-Ruiz MK,  Mundo  AF, Munoz-Pomer A,  Mohammed S, Cheng T,  Bolton E, Papatheodorou I, " +
        "Stein L, Ware D, and  Jaiswal P.";

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
        LOG.info("Deleted " + oldPathways.size() + " Pathway objects.");

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
            LOG.info("Stored new Gramene DataSource.");
        } else {
            LOG.info("Using existing Gramene DataSource.");
        }

        // get the existing Plant Reactome publication, if present
        Query qPublication = new Query();
        QueryClass qcPublication = new QueryClass(Publication.class);
        qPublication.addFrom(qcPublication);
        qPublication.addToSelect(qcPublication);
        QueryField publicationDOIField = new QueryField(qcPublication, "doi");
        QueryValue publicationDOIValue = new QueryValue(PLANT_REACTOME_PUB_DOI);
        qPublication.setConstraint(new SimpleConstraint(publicationDOIValue, ConstraintOp.EQUALS, publicationDOIField));
        // execute the query, populating Publication if present
        Publication tempPublication = null;
        Results publicationResults = osw.getObjectStore().execute(qPublication);
	for (Object resultObject : publicationResults.asList()) {
	    ResultsRow row = (ResultsRow) resultObject;
            tempPublication = (Publication) row.get(0);
	}
        // create and store the Gramene Publication if not already present
        if (tempPublication == null) {
            tempPublication = (Publication) DynamicUtil.createObject(Collections.singleton(Publication.class));
            tempPublication.setYear(PLANT_REACTOME_PUB_YEAR);
            tempPublication.setTitle(PLANT_REACTOME_PUB_TITLE);
            tempPublication.setDoi(PLANT_REACTOME_PUB_DOI);
            tempPublication.setJournal(PLANT_REACTOME_PUB_JOURNAL);
            tempPublication.setFirstAuthor(PLANT_REACTOME_PUB_FIRST_AUTHOR);
            osw.beginTransaction();
            osw.store(tempPublication);
            osw.commitTransaction();
            LOG.info("Stored new Plant Reactome Publication.");
        } else {
            LOG.info("Using existing Plant Reactome Publication.");
        }
        // need final for lambda expression below
        final Publication publication = tempPublication;
        
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
        LOG.info("Deleted " + oldDataSets.size() + " Plant Reactome DataSet objects.");
        
        // create and store the new Plant Reactome DataSet
        final DataSet dataSet = (DataSet) DynamicUtil.createObject(Collections.singleton(DataSet.class));
        dataSet.setPublication(publication);
        dataSet.setName(PLANT_REACTOME_NAME);
        dataSet.setUrl(PLANT_REACTOME_URL);
        dataSet.setDescription(PLANT_REACTOME_DESCRIPTION);
        dataSet.setLicence(PLANT_REACTOME_LICENCE);
        dataSet.setVersion(PLANT_REACTOME_VERSION);
        dataSet.setDataSource(dataSource);
        osw.beginTransaction();
        osw.store(dataSet);
        osw.commitTransaction();
        LOG.info("Stored new Plant Reactome DataSet object.");
        
        // load the Plant Reactome gene file into maps of identifier/name lists keyed by gene
        // 0=identifier         1=name                  2=species               3=gene
        // R-PVU-1119430.1	Chorismate biosynthesis	Phaseolus vulgaris	PHAVU_002G106700g
        Map<String,String> pathwayIdentifierName = new HashMap<>();                // pathway identifier to pathway name
        Map<String,Set<String>> ensemblNamePathwayIdentifiers = new HashMap<>();  // gene Ensembl name to set of pathway identifiers
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
                String ensemblName = fields[3];
                pathwayIdentifierName.put(pathwayIdentifier, pathwayName);
                if (ensemblNamePathwayIdentifiers.containsKey(ensemblName)) {
                    ensemblNamePathwayIdentifiers.get(ensemblName).add(pathwayIdentifier);
                } else {
                    Set<String> pathwayIdentifiers = new HashSet<>();
                    pathwayIdentifiers.add(pathwayIdentifier);
                    ensemblNamePathwayIdentifiers.put(ensemblName, pathwayIdentifiers);
                }
            }
        }
        
        // query all genes in the mine WITH Ensembl names and put into genes set
        Query qGene = new Query();
        QueryClass qcGene = new QueryClass(Gene.class);
        qGene.addFrom(qcGene);
        qGene.addToSelect(qcGene);
        // execute the query
        Results geneResults = osw.getObjectStore().execute(qGene);
        Set<Gene> genes = new HashSet<>();
	for (Object resultObject : geneResults.asList()) {
	    ResultsRow row = (ResultsRow) resultObject;
            Gene gene = (Gene) row.get(0);
            if (gene.getEnsemblName() != null) {
                genes.add(gene);
            }
	}

        ///////////////////////////////////////////////////////////////////////////////////////////////
        // populate gene-pathway identifier map and set of pathway identifiers to store
        Map<Gene,Set<String>> genePathwayIdentifiers  = new ConcurrentHashMap<>();
        Set<String> storedPathwayIdentifiers = new ConcurrentSkipListSet<>();
        genes.parallelStream().forEach(gene -> {
                String ensemblName = gene.getEnsemblName();
                if (ensemblNamePathwayIdentifiers.containsKey(ensemblName)) {
                    Set<String> identifiers = ensemblNamePathwayIdentifiers.get(ensemblName);
                    genePathwayIdentifiers.put(gene, identifiers);
                    storedPathwayIdentifiers.addAll(identifiers);
                }
            });
        ///////////////////////////////////////////////////////////////////////////////////////////////

        ///////////////////////////////////////////////////////////////////////////////////////////////
        // create our pathways associated with a gene in the mine and store in a map
        Map<String,Pathway> pathways = new ConcurrentHashMap<>();
        storedPathwayIdentifiers.parallelStream().forEach(identifier -> {
                Pathway pathway = (Pathway) DynamicUtil.createObject(Collections.singleton(Pathway.class));
                pathway.setPrimaryIdentifier(identifier);
                pathway.setName(pathwayIdentifierName.get(identifier));
                pathway.addDataSets(dataSet);
                pathway.addPublications(publication);
                pathways.put(identifier, pathway);
            });
        ///////////////////////////////////////////////////////////////////////////////////////////////
        
        // store the pathways
        osw.beginTransaction();
        for (Pathway pathway : pathways.values()) {
            osw.store(pathway);
        }
        osw.commitTransaction();
        LOG.info("Stored " + storedPathwayIdentifiers.size() + " Pathway objects.");

        // set the Gene.pathways collection
        osw.beginTransaction();
        for (Gene gene : genePathwayIdentifiers.keySet()) {
            Set<Pathway> genePathways = new HashSet<>();
            for (String identifier : genePathwayIdentifiers.get(gene)) {
                genePathways.add(pathways.get(identifier));
            }
            // NOTE: not sure why I have to use setFieldValue rather than addPathways or setPathways
            // when using cloneInterMineObject.
            Gene clone = PostProcessUtil.cloneInterMineObject(gene);
            clone.setFieldValue("pathways", genePathways);
            osw.store(clone);
        }
        osw.commitTransaction();
        LOG.info("Updated pathways collection for " + genePathwayIdentifiers.size() + " Gene objects.");
            
        // close the ObjectStoreWriter
        osw.close();
    }
    
}
