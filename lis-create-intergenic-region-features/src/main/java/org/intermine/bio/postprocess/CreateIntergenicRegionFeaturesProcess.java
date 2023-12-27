package org.intermine.bio.postprocess;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import org.intermine.metadata.ConstraintOp;
import org.intermine.metadata.Model;

import org.intermine.model.bio.Chromosome;
import org.intermine.model.bio.DataSet;
import org.intermine.model.bio.DataSource;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.IntergenicRegion;
import org.intermine.model.bio.Location;
import org.intermine.model.bio.Supercontig;
    
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.intermine.postprocess.PostProcessor;

import org.intermine.util.DynamicUtil;

/**
 * Methods for creating feature for intergenic regions.
 * Completely rewritten from Kim Rutherford version using sorted maps.
 *
 * @author Sam Hokin
 */
public class CreateIntergenicRegionFeaturesProcess extends PostProcessor {

    static final Logger LOG = Logger.getLogger(CreateIntergenicRegionFeaturesProcess.class);
    
    ObjectStore os;
    Model model;
    DataSet dataSet;
    DataSource dataSource;
    boolean storeDataSource = false;
    
    ConcurrentHashMap<String,IntergenicRegion> intergenicRegions = new ConcurrentHashMap<>(); // keyed by GenePair.key

    /**
     * Create a new instance
     *
     * @param osw object store writer
     */
    public CreateIntergenicRegionFeaturesProcess(ObjectStoreWriter osw) throws ObjectStoreException {
        super(osw);
        this.os = osw.getObjectStore();
        this.model = os.getModel();
        // post-processor DataSource may already exist
        DataSource testDataSource = (DataSource) DynamicUtil.createObject(Collections.singleton(DataSource.class));
        testDataSource.setName("InterMine post-processor");
        DataSource existingDataSource = os.getObjectByExample(testDataSource, Collections.singleton("name"));
        if (existingDataSource == null) {
            this.dataSource = testDataSource;
            storeDataSource = true;
        } else {
            this.dataSource = existingDataSource;
            storeDataSource = false;
        }
        // DataSet version is a timestamp
        this.dataSet = (DataSet) DynamicUtil.createObject(Collections.singleton(DataSet.class));
        this.dataSet.setName("InterMine intergenic regions");
        this.dataSet.setDescription("Intergenic regions created by the InterMine core post-processor");
        this.dataSet.setVersion("" + new Date()); // current time and date
        this.dataSet.setUrl("http://www.intermine.org");
        this.dataSet.setDataSource(dataSource);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {

        QueryClass qcIR = new QueryClass(IntergenicRegion.class);

        // query chromosomes that CONTAIN IntergenicRegions to SKIP processing them
        Set<Chromosome> chromosomesToSkip = new HashSet<>();
        Query qChromosome = new Query();
        QueryClass qcChromosome = new QueryClass(Chromosome.class);
        qChromosome.addFrom(qcChromosome);
        qChromosome.addToSelect(qcChromosome);
        qChromosome.addFrom(qcIR);
        QueryObjectReference irChrRef = new QueryObjectReference(qcIR, "chromosome");
        ContainsConstraint irChrConstraint = new ContainsConstraint(irChrRef, ConstraintOp.CONTAINS, qcChromosome);
        qChromosome.setConstraint(irChrConstraint);
        Results chrResults = os.execute(qChromosome, 1000, true, true, true);
        for (Object obj : chrResults.asList()) {
            ResultsRow rr = (ResultsRow) obj;
            Chromosome chromosome = (Chromosome) rr.get(0);
            chromosomesToSkip.add(chromosome);
        }

        // query supercontigs that CONTAIN IntergenicRegions to SKIP processing them
        Set<Supercontig> supercontigsToSkip = new HashSet<>();
        Query qSupercontig = new Query();
        QueryClass qcSupercontig = new QueryClass(Supercontig.class);
        qSupercontig.addFrom(qcSupercontig);
        qSupercontig.addToSelect(qcSupercontig);
        qSupercontig.addFrom(qcIR);
        QueryObjectReference irSupRef = new QueryObjectReference(qcIR, "supercontig");
        ContainsConstraint irSupConstraint = new ContainsConstraint(irSupRef, ConstraintOp.CONTAINS, qcSupercontig);
        qSupercontig.setConstraint(irSupConstraint);
        Results supResults = os.execute(qSupercontig, 1000, true, true, true);
        for (Object obj : supResults.asList()) {
            ResultsRow rr = (ResultsRow) obj;
            Supercontig supercontig = (Supercontig) rr.get(0);
            supercontigsToSkip.add(supercontig);
        }

        // put chromosomes that DON'T contain intergenic regions into a List for processing
        qChromosome = new Query();
        qChromosome.addFrom(qcChromosome);
        qChromosome.addToSelect(qcChromosome);
        qChromosome.addToOrderBy(new QueryField(qcChromosome, "primaryIdentifier"));
        List<Chromosome> chromosomesToProcess = new ArrayList<>();
        chrResults = os.execute(qChromosome, 1000, true, true, true);
        for (Object obj : chrResults.asList()) {
            ResultsRow rr = (ResultsRow) obj;
            Chromosome chromosome = (Chromosome) rr.get(0);
            if (!chromosomesToSkip.contains(chromosome)) {
                chromosomesToProcess.add(chromosome);
            }
        }
        LOG.info("Will process intergenic regions for " + chromosomesToProcess.size() + " chromosomes.");

        // put supercontigs that DON'T contain intergenic regions into a List for processing
        qSupercontig = new Query();
        qSupercontig.addFrom(qcSupercontig);
        qSupercontig.addToSelect(qcSupercontig);
        qSupercontig.addToOrderBy(new QueryField(qcSupercontig, "primaryIdentifier"));
        List<Supercontig> supercontigsToProcess = new ArrayList<>();
        chrResults = os.execute(qSupercontig, 1000, true, true, true);
        for (Object obj : chrResults.asList()) {
            ResultsRow rr = (ResultsRow) obj;
            Supercontig supercontig = (Supercontig) rr.get(0);
            if (!supercontigsToSkip.contains(supercontig)) {
                supercontigsToProcess.add(supercontig);
            }
        }
        LOG.info("Will process intergenic regions for " + supercontigsToProcess.size() + " supercontigs.");

        // create intergenic regions per chromosome
        // collect genes per chromosome/annotation since intergenic regions are annotation features
        for (Chromosome chromosome : chromosomesToProcess) {
            // query genes on this chromosome and load pairs into a TreeMap sorted by chromosomeLocation.start
            Query qGene = new Query();
            QueryClass qcGene = new QueryClass(Gene.class);
            qGene.addFrom(qcGene);
            qGene.addToSelect(qcGene);
            qGene.addToOrderBy(new QueryField(qcGene, "primaryIdentifier"));
            QueryObjectReference chromosomeRef = new QueryObjectReference(qcGene, "chromosome");
            ContainsConstraint geneOnChromosomeConstraint = new ContainsConstraint(chromosomeRef, ConstraintOp.CONTAINS, chromosome);
            qGene.setConstraint(geneOnChromosomeConstraint);
            Map<String,TreeMap<Integer,Gene>> annotationGenes = new HashMap<>(); // keyed by annotationVersion
            Results results = os.execute(qGene, 1000, true, true, true);
            for (Object obj : results.asList()) {
                ResultsRow rr = (ResultsRow) obj;
                Gene gene = (Gene) rr.get(0);
                String annotationVersion = gene.getAnnotationVersion();
                if (annotationGenes.containsKey(annotationVersion)) {
                    annotationGenes.get(annotationVersion).put(gene.getChromosomeLocation().getStart(), gene);
                } else {
                    TreeMap<Integer,Gene> genes = new TreeMap<>();
                    genes.put(gene.getChromosomeLocation().getStart(), gene);
                    annotationGenes.put(annotationVersion, genes);
                }
            }
            // create intergenic regions for each annotation version
            for (String annotationVersion : annotationGenes.keySet()) {
                TreeMap<Integer,Gene> genes = annotationGenes.get(annotationVersion);
                createIntergenicRegions(genes);
            }
        }

        // create intergenic regions per supercontig
        // collect genes per supercontig/annotation since intergenic regions are annotation features
        for (Supercontig supercontig : supercontigsToProcess) {
            // query genes on this supercontig and load into a TreeMap sorted by supercontigLocation.start
            Query qGene = new Query();
            QueryClass qcGene = new QueryClass(Gene.class);
            qGene.addFrom(qcGene);
            qGene.addToSelect(qcGene);
            qGene.addToOrderBy(new QueryField(qcGene, "primaryIdentifier"));
            QueryObjectReference supercontigRef = new QueryObjectReference(qcGene, "supercontig");
            ContainsConstraint geneOnSupercontigConstraint = new ContainsConstraint(supercontigRef, ConstraintOp.CONTAINS, supercontig);
            qGene.setConstraint(geneOnSupercontigConstraint);
            Map<String,TreeMap<Integer,Gene>> annotationGenes = new HashMap<>(); // keyed by annotationVersion
            Results results = os.execute(qGene, 1000, true, true, true);
            for (Object obj : results.asList()) {
                ResultsRow rr = (ResultsRow) obj;
                Gene gene = (Gene) rr.get(0);
                String annotationVersion = gene.getAnnotationVersion();
                if (annotationGenes.containsKey(annotationVersion)) {
                    annotationGenes.get(annotationVersion).put(gene.getSupercontigLocation().getStart(), gene);
                } else {
                    TreeMap<Integer,Gene> genes = new TreeMap<>();
                    genes.put(gene.getSupercontigLocation().getStart(), gene);
                    annotationGenes.put(annotationVersion, genes);
                }
            }
            // create intergenic regions for each annotation version
            for (String annotationVersion : annotationGenes.keySet()) {
                TreeMap<Integer,Gene> genes = annotationGenes.get(annotationVersion);
                createIntergenicRegions(genes);
            }
        }
        // store the intergenic regions
        if (intergenicRegions.size() > 0) {
            osw.beginTransaction();
            if (storeDataSource) osw.store(dataSource);
            osw.store(dataSet);
            // create a Set of adjacent genes (so we don't double-store the overlaps)
            Set<Gene> genes = new HashSet<>();
            for (IntergenicRegion ir : intergenicRegions.values()) {
                Set<Gene> adjacentGenes = ir.getAdjacentGenes();
                genes.addAll(adjacentGenes);
            }
            LOG.info("Storing " + intergenicRegions.size() + " IntergenicRegion and Location objects...");
            for (IntergenicRegion ir : intergenicRegions.values()) {
                Location location = null;
                if (onChromosome(ir)) {
                    location = ir.getChromosomeLocation();
                } else {
                    location = ir.getSupercontigLocation();
                }
                osw.store(ir);
                osw.store(location);
            }
            LOG.info("...done.");
            LOG.info("Storing " + genes.size() + " adjacent genes...");
            for (Gene gene : genes) {
                osw.store(gene);
            }
            LOG.info("...done.");
            LOG.info("Committing transaction...");
            osw.commitTransaction();
            LOG.info("...done.");
        }
    }

    /**
     * Create IntergenicRegions and Locations for a sorted map of genes on a single chromosome/supercontig.
     * N genes on a contig will result in N+1 intergenic regions.
     * Run in parallel stream by using GenePair objects.
     *
     * @param genes a TreeMap of genes keyed by start
     * @return a Set of IntergenicRegions
     */
    void createIntergenicRegions(TreeMap<Integer,Gene> genes) {
        // create a List of strand-specific GenePair objects for processing
        List<Gene> forwardGenes = new ArrayList<>();
        List<Gene> reverseGenes = new ArrayList<>();
        for (Gene gene : genes.values()) {
            if (onForwardStrand(gene)) {
                forwardGenes.add(gene);
            } else {
                reverseGenes.add(gene);
            }
        }
        if (forwardGenes.size() > 0) {
            // form GenePairs on forward strand
            final List<GenePair> forwardGenePairs = new ArrayList<>();
            Gene preceding = null; // first has no preceding gene
            for (Gene following : forwardGenes) {
                forwardGenePairs.add(new GenePair(preceding, following));
                preceding = following; // for next round
            }
            forwardGenePairs.add(new GenePair(preceding, null)); // last has no following gene
            //////////////////////////////////////////////////////////////////////////
            forwardGenePairs.parallelStream().forEach(pair -> {
                    IntergenicRegion ir = createIntergenicRegion(pair, dataSet);
                    if (ir != null) intergenicRegions.put(pair.key, ir);
                });
            //////////////////////////////////////////////////////////////////////////
        }
        if (reverseGenes.size() > 0) {
            // form GenePairs on reverse strand
            final List<GenePair> reverseGenePairs = new ArrayList<>();
            Gene preceding = null; // first has no preceding gene
            for (Gene following : reverseGenes) {
                reverseGenePairs.add(new GenePair(preceding, following));
                preceding = following; // for next round
            }
            reverseGenePairs.add(new GenePair(preceding, null)); // last has no following gene
            //////////////////////////////////////////////////////////////////////////
            reverseGenePairs.parallelStream().forEach(pair -> {
                    IntergenicRegion ir = createIntergenicRegion(pair, dataSet);
                    if (ir != null) intergenicRegions.put(pair.key, ir);
                });
            //////////////////////////////////////////////////////////////////////////
        }
    }

    /**
     * Create an IntergenicRegion along with its location and adjacent genes.
     *
     * @param pair a GenePair surrounding the intergenic region to be created
     * @return an IntergenicRegion object
     */
    static IntergenicRegion createIntergenicRegion(GenePair pair, DataSet dataSet) {
        // get a Gene that isn't null
        Gene gene = null;
        if (pair.preceding != null) {
            gene = pair.preceding;
        } else {
            gene = pair.following;
        }
        // region start and end
        int start = 0;
        if (pair.preceding == null) {
            start = 1;
        } else if (onChromosome(gene)) {
            start = pair.preceding.getChromosomeLocation().getEnd() + 1;
        } else {
            start = pair.preceding.getSupercontigLocation().getEnd() + 1;
        }
        int end = 0;
        if (pair.following == null && onChromosome(gene)) {
            end = pair.preceding.getChromosome().getLength();
        } else if (pair.following == null) {
            end = pair.preceding.getSupercontig().getLength();
        } else if (onChromosome(gene)) {
            end = pair.following.getChromosomeLocation().getStart() - 1;
        } else {
            end = pair.following.getSupercontigLocation().getStart() - 1;
        }
        // this happpens when two genes overlap on the same strand
        if (start >= end) return null;
        // create Location
        Location location = (Location) DynamicUtil.createObject(Collections.singleton(Location.class));
        location.setStart(start);
        location.setEnd(end);
        location.setStrand("1");
        location.addDataSets(dataSet);
        if (onChromosome(gene)) {
            location.setLocatedOn(gene.getChromosome());
        } else {
            location.setLocatedOn(gene.getSupercontig());
        }
        // create IntergenicRegion
        IntergenicRegion intergenicRegion = (IntergenicRegion) DynamicUtil.createObject(Collections.singleton(IntergenicRegion.class));
        location.setFeature(intergenicRegion);
        if (onChromosome(gene)) {
            intergenicRegion.setChromosomeLocation(location);
            intergenicRegion.setChromosome(gene.getChromosome());
            intergenicRegion.setOrganism(gene.getChromosome().getOrganism());
            intergenicRegion.setStrain(gene.getChromosome().getStrain());
        } else {
            intergenicRegion.setSupercontigLocation(location);
            intergenicRegion.setSupercontig(gene.getSupercontig());
            intergenicRegion.setOrganism(gene.getSupercontig().getOrganism());
            intergenicRegion.setStrain(gene.getSupercontig().getStrain());
        }
        intergenicRegion.setPrimaryIdentifier(formPrimaryIdentifier(pair));
        intergenicRegion.setSecondaryIdentifier(formSecondaryIdentifier(pair));
        intergenicRegion.setName(formName(pair));
        intergenicRegion.setAssemblyVersion(gene.getAssemblyVersion());
        intergenicRegion.setAnnotationVersion(gene.getAnnotationVersion());
        if (pair.preceding == null) {
            intergenicRegion.setDescription("Intergenic region between start and " + gene.getName());
        } else if (pair.following == null) {
            intergenicRegion.setDescription("Intergenic region between " + gene.getName() + " and end");
        } else {
            intergenicRegion.setDescription("Intergenic region between " + pair.preceding.getName() + " and " + pair.following.getName());
        }
        intergenicRegion.addDataSets(dataSet);
        intergenicRegion.setLength(location.getEnd() - location.getStart() + 1);
        // adjacent genes
        if (pair.preceding != null) {
            if (isPositiveStrand(pair.preceding)) {
                pair.preceding.setDownstreamIntergenicRegion(intergenicRegion);
            } else {
                pair.preceding.setUpstreamIntergenicRegion(intergenicRegion);
            }
            intergenicRegion.addAdjacentGenes(pair.preceding);
        }
        if (pair.following != null) {
            if (isPositiveStrand(pair.following)) {
                pair.following.setUpstreamIntergenicRegion(intergenicRegion);
            } else {
                pair.following.setDownstreamIntergenicRegion(intergenicRegion);
            }
            intergenicRegion.addAdjacentGenes(pair.following);
        }
        // return
        return intergenicRegion;
    }

    /**
     * Return true if the given Gene is on the positive strand.
     */
    static boolean isPositiveStrand(Gene gene) {
        if (gene.getChromosome() != null) {
            return gene.getChromosomeLocation().getStrand() == null || gene.getChromosomeLocation().getStrand().equals("1");
        } else {
            return gene.getSupercontigLocation().getStrand() == null || gene.getSupercontigLocation().getStrand().equals("1");
        }
    }

    /**
     * Utility to form the primaryIndentifier for an IntergenicRegion.
     *
     * @param pair a GenePair surrounding the intergenic region
     * @return a primaryIdentifier string
     */
    static String formPrimaryIdentifier(GenePair pair) {
        String identifier = "";
        if (pair.preceding != null) identifier += pair.preceding.getPrimaryIdentifier();
        identifier += "|";
        if (pair.following != null) identifier += pair.following.getPrimaryIdentifier();
        return identifier;
    }

    /**
     * Utility to form the secondaryIdentifier for an IntergenicRegion.
     *
     * @param pair a GenePair surrounding the intergenic region
     * @return a secondaryIdentifier string
     */
    static String formSecondaryIdentifier(GenePair pair) {
        String identifier = "";
        if (pair.preceding != null) identifier += pair.preceding.getSecondaryIdentifier();
        identifier += "|";
        if (pair.following != null) identifier += pair.following.getSecondaryIdentifier();
        return identifier;
    }

    /**
     * Utility to form the name for an IntergenicRegion.
     *
     * @param pair a GenePair surrounding the intergenic region
     * @return a name string
     */
    static String formName(GenePair pair) {
        String name = "";
        if (pair.preceding != null) name += pair.preceding.getName();
        name += "|";
        if (pair.following != null) name += pair.following.getName();
        return name;
    }

    /**
     * Return true if the provided gene is on a Chromosome (not a Supercontig).
     *
     * @param gene a Gene object
     * @return true if the Gene is on a Chromosome
     */
    static boolean onChromosome(Gene gene) {
        return (gene.getChromosome() != null);
    }

    /**
     * Return true if the provided intergenic region is on a Chromosome (not a Supercontig).
     *
     * @param gene a IntergenicRegion object
     * @return true if the region is on a Chromosome
     */
    static boolean onChromosome(IntergenicRegion region) {
        return (region.getChromosome() != null);
    }

    /**
     * Return true if the given gene is on the forward strand.
     *
     * @param gene a Gene object
     * @return true if the gene is on the forward strand
     */
    static boolean onForwardStrand(Gene gene) {
        if (onChromosome(gene)) {
            return (gene.getChromosomeLocation().getStrand() == null || gene.getChromosomeLocation().getStrand().equals("1"));
        } else {
            return (gene.getSupercontigLocation().getStrand() == null || gene.getSupercontigLocation().getStrand().equals("1"));
        }
    }

    /**
     * Encapsulates the pair of genes around an intergenic region.
     * Either one (but not both) may be null at the beginning/end of the chromosome/supercontig.
     * Both genes must be on same strand!
     */
    static class GenePair {
        Gene preceding; // the gene preceding the intergenic region
        Gene following; // the gene following the intergenic region
        String key;     // unique key based on gene identifiers
        GenePair(Gene preceding, Gene following) {
            this.preceding = preceding;
            this.following = following;
            key = formPrimaryIdentifier(this);
        }
    }

}
