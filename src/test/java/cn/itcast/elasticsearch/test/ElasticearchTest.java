package cn.itcast.elasticsearch.test;


import cn.itcast.elasticsearch.ItemRepository;
import cn.itcast.elasticsearch.pojo.Item;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@SpringBootTest
@RunWith(SpringRunner.class)
public class ElasticearchTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    private ItemRepository itemRepository;


    @Test
    public void testIndex(){
        this.elasticsearchTemplate.createIndex(Item.class);
        this.elasticsearchTemplate.putMapping(Item.class);
    }

    @Test
    public void testCreat(){
        Item item = new Item(1L, "xiaomi7", " téléphone",
                "xiaomi", 3499.00, "http://image.leyou.com/13123.jpg");
        this.itemRepository.save(item);

    }

    @Test
    public void indexList() {
        List<Item> list = new ArrayList<>();
        list.add(new Item(2L, "jianguoR1", " téléphone", "chuizi", 3699.00, "http://image.leyou.com/123.jpg"));
        list.add(new Item(3L, "huaweiMETA10", " téléphone", "huawei", 4499.00, "http://image.leyou.com/3.jpg"));
        // Effectuer des ajouts en lot à partir d'une collection d'objets
        this.itemRepository.saveAll(list);
    }

    @Test
    public void testQuery(){
        Optional<Item> optional = this.itemRepository.findById(1l);
        System.out.println(optional.get());
    }

    @Test
    public void testFind(){
        // Rechercher tous les éléments et les trier par ordre décroissant de prix
        Iterable<Item> items = this.itemRepository.findAll(Sort.by(Sort.Direction.DESC, "price"));
        items.forEach(item-> System.out.println(item));
    }




    @Test
    public void indexListOs() {
        List<Item> list1 = new ArrayList<>();
        list1.add(new Item(4L, "xiaomi7", "téléphone", "xiaomi", 3299.00, "http://image.leyou.com/13123.jpg"));
        list1.add(new Item(5L, "jianguoR1", "téléphone", "chuizi", 3699.00, "http://image.leyou.com/13123.jpg"));
        list1.add(new Item(6L, "huaweiMETA10", "téléphone", "huawei", 4499.00, "http://image.leyou.com/13123.jpg"));
        list1.add(new Item(7L, "xiaomiMix2S", "téléphone", "xiaomi", 4299.00, "http://image.leyou.com/13123.jpg"));
        list1.add(new Item(8L, "honorV10", "téléphone", "huawei", 2799.00, "http://image.leyou.com/13123.jpg"));
        // Recevoir une collection d'objets et effectuer des ajouts en lot
        this.itemRepository.saveAll(list1);
    }

    @Test
    public void queryByPriceBetween(){
        List<Item> list = this.itemRepository.findByPriceBetween(2000.00, 3500.00);
        for (Item item : list) {
            System.out.println("item = " + item);
        }
    }


    @Test
    public void testQuery1(){
        // recherche par titre
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "xiaomi");
        Iterable<Item> items = this.itemRepository.search(queryBuilder);
        items.forEach(System.out::println);
    }

    @Test
    public void testNativeQuery(){
        // Créer des critères de recherche
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // Ajouter une recherche de base par segmentation
        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "xiaomi"));
        // Procéder à une recherche et récupérer les résultats
        Page<Item> items = this.itemRepository.search(queryBuilder.build());
        // Afficher le nombre total d'éléments
        System.out.println(items.getTotalElements());
        // Afficher le nombre total de pages
        System.out.println(items.getTotalPages());
        items.forEach(System.out::println);
    }

    @Test
    public void testNativeQuery1(){
        // Élaborer des critères de rechercher
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // Intégrer une recherche de segmentation de base
        queryBuilder.withQuery(QueryBuilders.termQuery("category", "téléphone"));

        // Initialiser les paramètres de pagination
        int page = 0;
        int size = 3;
        // Définir les paramètres de pagination
        queryBuilder.withPageable(PageRequest.of(page, size));

        // Effectuer une recherche et récupérer les résultats
        Page<Item> items = this.itemRepository.search(queryBuilder.build());
        // Afficher le nombre total d'éléments
        System.out.println(items.getTotalElements());
        // Afficher le nombre total de pages
        System.out.println(items.getTotalPages());
        // Nombre d'éléments par page
        System.out.println(items.getSize());
        // Page actuelle
        System.out.println(items.getNumber());
        items.forEach(System.out::println);
    }

    @Test
    public void testSort(){
        // Élaborer des critères de rechercher
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // Intégrer une recherche de segmentation de base
        queryBuilder.withQuery(QueryBuilders.termQuery("category", "手机"));

        // tri
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));

        // Effectuer une recherche et récupérer les résultats
        Page<Item> items = this.itemRepository.search(queryBuilder.build());
        // Afficher le nombre total d'éléments
        System.out.println(items.getTotalElements());
        items.forEach(System.out::println);
    }

    @Test
    public void testAgg(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // aucune source de données ne sera explicitement exclue
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));
        // 1、Ajouter une nouvelle agrégation, de type 'terms', nommée 'brands', basée sur le champ 'brand'
        queryBuilder.addAggregation(
                AggregationBuilders.terms("brands").field("brand"));
        // 2、La requête nécessite une conversion forcée des résultats en type AggregatedPage
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>) this.itemRepository.search(queryBuilder.build());
        // 3、analyer
        // 3.1、Extrait l'agrégation appelée 'brands' des résultats，
        // Parce que l'agrégation de type 'term' est réalisée en utilisant un champ de type String,
        // les résultats doivent être forcés à être convertis en type StringTerm
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        // 3.2、obtenir les buckets
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        // 3.3、itérer
        for (StringTerms.Bucket bucket : buckets) {
            // 3.4、Obtenir la clé du seau, c'est-à-dire le nom de la marque
            System.out.println(bucket.getKeyAsString());
            // 3.5、Récupérer le nombre de documents dans le seau
            System.out.println(bucket.getDocCount());
        }

    }

    @Test
    public void testSubAgg(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));
        queryBuilder.addAggregation(
                AggregationBuilders.terms("brands").field("brand")
                        .subAggregation(AggregationBuilders.avg("priceAvg").field("price")) // 在品牌聚合桶内进行嵌套聚合，求平均值
        );
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>) this.itemRepository.search(queryBuilder.build());
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        for (StringTerms.Bucket bucket : buckets) {
            // Obtenir la clé du seau, c'est-à-dire le nom de la marque
            // Obtenir le nombre de documents dans le seau
            System.out.println(bucket.getKeyAsString() + "，共" + bucket.getDocCount() + "台");

            // Obtenir les résultats de la sous-agrégation
            InternalAvg avg = (InternalAvg) bucket.getAggregations().asMap().get("priceAvg");
            System.out.println("Prix de vente moyen：" + avg.getValue());
        }

    }

}
