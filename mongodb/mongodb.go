package mongodb

import (
	"context"
	"fmt"
	"github.com/cihub/seelog"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"operate_mongo/config"
	"testing"
	"time"
)

func requireCursorLength(t *testing.T, cursor *mongo.Cursor, length int) {
	i := 0
	for cursor.Next(context.Background()) {
		i++
	}

	require.NoError(t, cursor.Err())
	require.Equal(t, i, length)
}

func InsertSpecifiedDocs() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(context.Background(),
		options.Client().ApplyURI("mongodb://"+config.GetConfig().HostPort))
	if err != nil {
		panic(err)
	}

	defer client.Disconnect(ctx)

	db := client.Database(config.GetConfig().MongoInsert.DataBase)

	coll := db.Collection(config.GetConfig().MongoInsert.Collection)

	insertCount := 0

	for i := 1; i <= config.GetConfig().MongoInsert.InsertCount; i++ {
		_, err = coll.InsertOne(
			context.Background(),
			bson.D{
				{"msgId", "d5e60b7edd0c4fc9a17cb83022d718a2"},
				{"fromId", "3584299134fa4975a9fd4a509898cfb4"},
				{"toId", "11042"},
				{"msgData", "Tokyo_London_NewYork"},
				{config.GetConfig().MongoInsert.Field1.Key, config.GetConfig().MongoInsert.Field1.Value},
				{config.GetConfig().MongoInsert.Field2.Key, config.GetConfig().MongoInsert.Field2.Value + i},
				{"isChatDeliver", 1},
			})
		if err != nil {
			seelog.Error("InsertOne_Err:", err)
			continue
		}
		insertCount++
	}

	seelog.Infof("SuccessInsertCount = %d\n", insertCount)
}

// InsertExamples contains examples for insert operations.
func InsertExamples(t *testing.T, db *mongo.Database) {
	coll := db.Collection("msg_svr")

	//err := coll.Drop(context.Background())
	//require.NoError(t, err)

	//{
	n := 1552967597762

	for i := 1; i <= 10; i++ {
		result, err := coll.InsertOne(
			context.Background(),
			bson.D{
				{"msgId", "d5e60b7edd0c4fc9a17cb83022d718a2"},
				{"fromId", "3584299134fa4975a9fd4a509898cfb4"},
				{"toId", "11042"},
				{"msgData", ""},
				{"createTime", n + i},
				{"isChatDeliver", 1},
			})

		require.NoError(t, err)
		require.NotNil(t, result.InsertedID)
	}

	//}

	//{
	//	cursor, err := coll.Find(
	//		context.Background(),
	//		bson.D{{"item", "canvas"}},
	//	)
	//
	//	require.NoError(t, err)
	//	requireCursorLength(t, cursor, 1)
	//
	//}
	//
	//{
	//	result, err := coll.InsertMany(
	//		context.Background(),
	//		[]interface{}{
	//			bson.D{
	//				{"item", "journal"},
	//				{"qty", int32(25)},
	//				{"tags", bson.A{"blank", "red"}},
	//				{"size", bson.D{
	//					{"h", 14},
	//					{"w", 21},
	//					{"uom", "cm"},
	//				}},
	//			},
	//			bson.D{
	//				{"item", "mat"},
	//				{"qty", int32(25)},
	//				{"tags", bson.A{"gray"}},
	//				{"size", bson.D{
	//					{"h", 27.9},
	//					{"w", 35.5},
	//					{"uom", "cm"},
	//				}},
	//			},
	//			bson.D{
	//				{"item", "mousepad"},
	//				{"qty", 25},
	//				{"tags", bson.A{"gel", "blue"}},
	//				{"size", bson.D{
	//					{"h", 19},
	//					{"w", 22.85},
	//					{"uom", "cm"},
	//				}},
	//			},
	//		})
	//
	//	require.NoError(t, err)
	//	require.Len(t, result.InsertedIDs, 3)
	//}
}

func DeleteExamples(t *testing.T, db *mongo.Database) {
	coll := db.Collection("inventory_delete")

	err := coll.Drop(context.Background())
	require.NoError(t, err)

	{
		docs := []interface{}{
			bson.D{
				{"item", "journal"},
				{"qty", 25},
				{"size", bson.D{
					{"h", 14},
					{"w", 21},
					{"uom", "cm"},
				}},
				{"status", "A"},
			},
			bson.D{
				{"item", "notebook"},
				{"qty", 50},
				{"size", bson.D{
					{"h", 8.5},
					{"w", 11},
					{"uom", "in"},
				}},
				{"status", "P"},
			},
			bson.D{
				{"item", "paper"},
				{"qty", 100},
				{"size", bson.D{
					{"h", 8.5},
					{"w", 11},
					{"uom", "in"},
				}},
				{"status", "D"},
			},
			bson.D{
				{"item", "paper1"},
				{"qty", 100},
				{"size", bson.D{
					{"h", 8.5},
					{"w", 11},
					{"uom", "in"},
				}},
				{"status", "D"},
			},
			bson.D{
				{"item", "paper2"},
				{"qty", 100},
				{"size", bson.D{
					{"h", 8.5},
					{"w", 11},
					{"uom", "in"},
				}},
				{"status", "D"},
			},
			bson.D{
				{"item", "paper3"},
				{"qty", 100},
				{"size", bson.D{
					{"h", 8.5},
					{"w", 11},
					{"uom", "in"},
				}},
				{"status", "D"},
			},
			bson.D{
				{"item", "paper4"},
				{"qty", 100},
				{"size", bson.D{
					{"h", 8.5},
					{"w", 11},
					{"uom", "in"},
				}},
				{"status", "D"},
			},
			bson.D{
				{"item", "planner"},
				{"qty", 75},
				{"size", bson.D{
					{"h", 22.85},
					{"w", 30},
					{"uom", "cm"},
				}},
				{"status", "D"},
			},
			bson.D{
				{"item", "postcard"},
				{"qty", 45},
				{"size", bson.D{
					{"h", 10},
					{"w", 15.25},
					{"uom", "cm"},
				}},
				{"status", "A"},
			},
		}

		result, err := coll.InsertMany(context.Background(), docs)

		require.NoError(t, err)
		//require.Len(t, result.InsertedIDs, 5)

		log.Println("InsertedCount:", result.InsertedIDs)
	}

	{
		result, err := coll.DeleteMany(
			context.Background(),
			bson.D{
				{"status", "A"},
			},
		)

		require.NoError(t, err)
		require.Equal(t, int64(2), result.DeletedCount)

		log.Println("DeletedCount2:", result.DeletedCount)
	}

	{
		//result, err := coll.DeleteOne(
		//	context.Background(),
		//	bson.D{
		//		{"status", "D"},
		//	},
		//)
		//
		//require.NoError(t, err)
		//require.Equal(t, int64(1), result.DeletedCount)
		//
		//log.Println("DeletedCount1:", result.DeletedCount)

		cursor, _ := coll.Find(
			context.Background(),
			bson.D{{"status", "D"}},
		)

		i := 0
		for cursor.Next(context.Background()) {
			i++
			result, _ := coll.DeleteOne(
				context.Background(),
				bson.D{
					{"status", "D"},
				},
			)

			fmt.Printf("num: %d, deleted: %d\n", i, result.DeletedCount)
		}
	}

	//{
	//	result, err := coll.DeleteMany(context.Background(), bson.D{})
	//
	//	require.NoError(t, err)
	//	require.Equal(t, int64(2), result.DeletedCount)
	//
	//	log.Println("DeletedCount_last:", result.DeletedCount)
	//}
}

func DeleteRangeExamples(t *testing.T, db *mongo.Database) {
	coll := db.Collection("inventory_insert")

	//err := coll.Drop(context.Background())
	//require.NoError(t, err)

	//result, _ := coll.DeleteMany(
	//	context.Background(),
	//	bson.D{
	//		{"msgTime",bson.D{{"$gt",1559126249078}}},
	//	},
	//)
	result, _ := coll.DeleteMany(
		context.Background(),
		bson.D{
			{"item", "NewYork"},
			//{"radioId","2717"},
			{"qty", bson.D{{"$gt", 25}}},
		},
	)
	//cursor, _ := coll.Find(
	//	context.Background(),
	//	bson.M{
	//				//{"fromId", "10025"},
	//				//{"radioId","2717"},
	//				"msgTime":bson.M{"$lt":1559126249078},
	//			},
	//)
	//
	//for cursor.Next(context.Background()) {
	//	//i++
	//	//result, _ := coll.DeleteOne(
	//	//	context.Background(),
	//	//	bson.D{
	//	//		{"status", "D"},
	//	//	},
	//	//)
	//	fmt.Println("----")
	//	break
	//}
	//
	//require.NoError(t, err)
	//require.Equal(t, int64(2), result.DeletedCount)

	log.Println("deletedCount = :", result.DeletedCount)
}

func DeleteSpecifiedDocs() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(context.Background(),
		options.Client().ApplyURI("mongodb://"+config.GetConfig().HostPort))
	//options.Client().ApplyURI("mongodb://127.0.0.1:12345"))
	if err != nil {
		panic(err)
	}

	defer client.Disconnect(ctx)

	db := client.Database(config.GetConfig().MongoDel.DataBase)

	coll := db.Collection(config.GetConfig().MongoDel.Collection)

	//first, by days delete data
	isKey := config.GetConfig().MongoDel.Field.Key == ""
	isValue := config.GetConfig().MongoDel.Field.Value == 0
	if config.GetConfig().MongoDel.Days != 0 && isKey && isValue {
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByDay_Panic:", err)
			panic(err)
		}

		deleteDataByDay(coll, ts)
	}

	//second, by some fields delete data
	if config.GetConfig().MongoDel.Days == 0 && !isKey && !isValue {
		deleteDataBySomeFields(coll)
	}

	//third, by fields and days
	if config.GetConfig().MongoDel.Days != 0 && !isKey && !isValue {
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByFieldsAndDays_Panic:", err)
			panic(err)
		}

		deleteDataByFieldsAndDays(coll, ts)
	}

	return nil
}

func dayToTimeStamp() (int64, error) {
	timeStr := time.Now().Format("2006-01-02")

	t, err := time.ParseInLocation("2006-01-02", timeStr, time.Local)
	if err != nil {
		seelog.Error("ParseTime Err:", err)
		return 0, err
	}

	return t.AddDate(0, 0, -config.GetConfig().MongoDel.Days).UnixNano() / 1e6, nil
}

func deleteDataByDay(coll *mongo.Collection, ts int64) error {
	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.TimeKey, bson.D{{"$lte", ts}}},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByDay ERR:", err)
	}

	seelog.Info("DeletedCountByDays:", result.DeletedCount)

	return nil
}

func deleteDataBySomeFields(coll *mongo.Collection) error {
	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field.Key, config.GetConfig().MongoDel.Field.Value},
		},
	)
	if err != nil {
		seelog.Error("deleteDataBySomeFields ERR:", err)

		return err
	}

	seelog.Info("DeletedCountBySomeFields:", result.DeletedCount)

	return nil
}

func deleteDataByFieldsAndDays(coll *mongo.Collection, ts int64) error {
	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field.Key, config.GetConfig().MongoDel.Field.Value},
			{config.GetConfig().MongoDel.TimeKey, bson.D{{"$lte", ts}}},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByFieldsAndDays ERR:", err)

		return err
	}

	seelog.Info("DeletedCountByFieldsAndDays:", result.DeletedCount)

	return nil
}
