package xk6_mongo

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/mongo".
func init() {
	modules.Register("k6/x/mongo", New())
}

type (
	// RootModule is the global module instance that will create module
	// instances for each VU.
	RootModule struct{}

	// ModuleInstance represents an instance of the JS module.
	ModuleInstance struct {
		// vu provides methods for accessing internal k6 objects for a VU
		vu modules.VU
		// comparator is the exported type
		mongo *Mongo
	}
)

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Instance = &ModuleInstance{}
	_ modules.Module   = &RootModule{}
)

// New returns a pointer to a new RootModule instance.
func New() *RootModule {
	return &RootModule{}
}

// NewModuleInstance implements the modules.Module interface returning a new instance for each VU.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ModuleInstance{
		vu:    vu,
		mongo: &Mongo{vu: vu},
	}
}

// Exports implements the modules.Instance interface and returns the exported types for the JS module.
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{
		Default: mi.mongo,
	}
}

// Mongo is the k6 extension for a Mongo client.
type Mongo struct {
	vu modules.VU
}

// Client is the Mongo client wrapper.
type Client struct {
	client *mongo.Client
	vu     modules.VU
}

type UpsertOneModel struct {
	Query  interface{} `json:"query"`
	Update interface{} `json:"update"`
}

// NewClient represents the Client constructor (i.e. `new mongo.Client()`) and
// returns a new Mongo client object.
// connURI -> mongodb://username:password@address:port/db?connect=direct
func (m *Mongo) NewClient(connURI string) *Client {
	log.Print("start creating new client")

	clientOptions := options.Client().ApplyURI(connURI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Printf("Error while establishing a connection to MongoDB: %v", err)
		return nil
	}

	log.Print("created new client")
	return &Client{client: client, vu: m.vu}
}

func (c *Client) Insert(database string, collection string, doc interface{}) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.InsertOne(context.Background(), doc)
	if err != nil {
		log.Printf("Error while inserting document: %v", err)
		return err
	}
	log.Print("Document inserted successfully")
	c.pushDataSentMetric(doc)
	return nil
}

func (c *Client) InsertMany(database string, collection string, docs []interface{}) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.InsertMany(context.Background(), docs)
	if err != nil {
		log.Printf("Error while inserting multiple documents: %v", err)
		return err
	}
	c.pushDataSentMetric(docs)
	return nil
}

func (c *Client) Upsert(database string, collection string, filter interface{}, upsert interface{}) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	opts := options.Update().SetUpsert(true)
	_, err := col.UpdateOne(context.Background(), filter, upsert, opts)
	if err != nil {
		log.Printf("Error while performing upsert: %v", err)
		return err
	}
	return nil
}

func (c *Client) Find(database string, collection string, filter interface{}, sort interface{}, limit int64) ([]bson.M, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	opts := options.Find().SetSort(sort).SetLimit(limit)
	cur, err := col.Find(context.Background(), filter, opts)
	if err != nil {
		log.Printf("Error while finding documents: %v", err)
		return nil, err
	}
	var results []bson.M
	if err = cur.All(context.Background(), &results); err != nil {
		log.Printf("Error while decoding documents: %v", err)
		return nil, err
	}

	c.pushDataReceivedMetric(results)
	return results, nil
}

func (c *Client) Aggregate(database string, collection string, pipeline interface{}) ([]bson.M, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	cur, err := col.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Printf("Error while aggregating: %v", err)
		return nil, err
	}
	var results []bson.M
	if err = cur.All(context.Background(), &results); err != nil {
		log.Printf("Error while decoding documents: %v", err)
		return nil, err
	}

	c.pushDataReceivedMetric(results)
	return results, nil
}

func (c *Client) FindOne(database string, collection string, filter map[string]string) (bson.M, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	var result bson.M
	err := col.FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		log.Printf("Error while finding the document: %v", err)
		return nil, err
	}

	c.pushDataReceivedMetric([]bson.M{result})
	return result, nil
}

func (c *Client) UpdateOne(database string, collection string, filter interface{}, data bson.D) error {
	db := c.client.Database(database)
	col := db.Collection(collection)

	_, err := col.UpdateOne(context.Background(), filter, data)
	if err != nil {
		log.Printf("Error while updating the document: %v", err)
		return err
	}

	return nil
}

func (c *Client) UpdateMany(database string, collection string, filter interface{}, data bson.D) error {
	db := c.client.Database(database)
	col := db.Collection(collection)

	update := bson.D{{"$set", data}}

	_, err := col.UpdateMany(context.Background(), filter, update)
	if err != nil {
		log.Printf("Error while updating the documents: %v", err)
		return err
	}

	return nil
}

func (c *Client) FindAll(database string, collection string) ([]bson.M, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	cur, err := col.Find(context.Background(), bson.D{{}})
	if err != nil {
		log.Printf("Error while finding documents: %v", err)
		return nil, err
	}

	var results []bson.M
	if err = cur.All(context.Background(), &results); err != nil {
		log.Printf("Error while decoding documents: %v", err)
		return nil, err
	}

	c.pushDataReceivedMetric(results)
	return results, nil
}

func (c *Client) DeleteOne(database string, collection string, filter map[string]string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.DeleteOne(context.Background(), filter)
	if err != nil {
		log.Printf("Error while deleting the document: %v", err)
		return err
	}

	return nil
}

func (c *Client) DeleteMany(database string, collection string, filter map[string]string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.DeleteMany(context.Background(), filter)
	if err != nil {
		log.Printf("Error while deleting the documents: %v", err)
		return err
	}

	return nil
}

func (c *Client) Distinct(database string, collection string, field string, filter interface{}) ([]interface{}, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	result, err := col.Distinct(context.Background(), field, filter)
	if err != nil {
		log.Printf("Error while getting distinct values: %v", err)
		return nil, err
	}

	return result, nil
}

func (c *Client) DropCollection(database string, collection string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	err := col.Drop(context.Background())
	if err != nil {
		log.Printf("Error while dropping the collection: %v", err)
		return err
	}

	return nil
}

func (c *Client) CountDocuments(database string, collection string, filter interface{}) (int64, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	count, err := col.CountDocuments(context.Background(), filter)
	if err != nil {
		log.Printf("Error while counting documents: %v", err)
		return 0, err
	}
	return count, nil
}

func (c *Client) FindOneAndUpdate(database string, collection string, filter interface{}, update interface{}) (*mongo.SingleResult, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	result := col.FindOneAndUpdate(context.Background(), filter, update, opts)
	if result.Err() != nil {
		log.Printf("Error while finding and updating document: %v", result.Err())
		return nil, result.Err()
	}
	return result, nil
}

func (c *Client) Disconnect() error {
	err := c.client.Disconnect(context.Background())
	if err != nil {
		log.Printf("Error while disconnecting from the database: %v", err)
		return err
	}

	return nil
}

func getSizeBytes(docOrDocs interface{}) (int64, error) {
	totalBytes := int64(0)
	switch v := docOrDocs.(type) {
	case map[string]interface{}, bson.M: // Single document
		bytes, err := bson.Marshal(v)
		if err != nil {
			log.Printf("Error while marshaling single document: %v", err)
			return 0, err
		}
		totalBytes = int64(len(bytes))
	case []interface{}:
		for _, doc := range v {
			bytes, err := bson.Marshal(doc)
			if err != nil {
				log.Printf("Error while marshaling one of multiple documents: %v", err)
				return 0, err
			}
			totalBytes += int64(len(bytes))
		}
	default:
		return 0, fmt.Errorf("unsupported type for calculating size: %T", v)
	}
	return totalBytes, nil
}

func (c *Client) pushDataSentMetric(docOrDocs interface{}) error {
	bytesSent, err := getSizeBytes(docOrDocs)
	if err != nil {
		log.Printf("Error calculating request size: %v", err)
		return err
	}
	state := c.vu.State()
	dataSentMetric := state.BuiltinMetrics.DataSent
	go metrics.PushIfNotDone(c.vu.Context(), state.Samples, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				TimeSeries: metrics.TimeSeries{
					Metric: dataSentMetric,
					Tags:   state.Tags.GetCurrentValues().Tags,
				},
				Value: float64(bytesSent),
				Time:  time.Now().UTC(),
			},
		},
	})
	return nil
}

func (c *Client) pushDataReceivedMetric(results []bson.M) error {
	bytesReceived, err := getSizeBytes(results)
	if err != nil {
		log.Printf("Error calculating response size: %v", err)
		return err
	}
	state := c.vu.State()
	dataReceivedMetric := state.BuiltinMetrics.DataReceived
	go metrics.PushIfNotDone(c.vu.Context(), state.Samples, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				TimeSeries: metrics.TimeSeries{
					Metric: dataReceivedMetric,
					Tags:   state.Tags.GetCurrentValues().Tags,
				},
				Value: float64(bytesReceived),
				Time:  time.Now().UTC(),
			},
		},
	})
	return nil
}
