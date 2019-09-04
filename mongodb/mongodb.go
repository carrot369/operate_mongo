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
		var isPull bool
		if config.GetConfig().MongoInsert.Field3.Value == 1 {
			isPull = true
		} else if config.GetConfig().MongoInsert.Field3.Value == 2 {
			isPull = false
		}

		_, err = coll.InsertOne(
			context.Background(),
			bson.D{
				{config.GetConfig().MongoInsert.Field4.Key, config.GetConfig().MongoInsert.Field4.Value},
				{"fromId", "3584299134fa4975a9fd4a509898cfb4"},
				{"toId", "11042"},
				{"msgData", "Tokyo_London_NewYork"},
				{config.GetConfig().MongoInsert.Field1.Key, config.GetConfig().MongoInsert.Field1.Value},
				{config.GetConfig().MongoInsert.Field2.Key, config.GetConfig().MongoInsert.Field2.Value + i},
				{"isChatDeliver", 1},
				{config.GetConfig().MongoInsert.Field3.Key, isPull},
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

	for i := 1; i <= 5; i++ {
		result, err := coll.InsertOne(
			context.Background(),
			bson.D{
				{"msgId", "d5e60b7edd0c4fc9a17cb83022d718a2"},
				{"fromId", "3584299134fa4975a9fd4a509898cfb4"},
				{"toId", "11042"},
				{"msgData", ""},
				{"createTime", n + i},
				{"isChatDeliver", 1},
				{"bPulled", true},
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
	coll := db.Collection("msg_svr")

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
			//{"item", "NewYork"},
			//{"radioId","2717"},
			{"bPulled", bson.D{{"$exists", true}}},
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

	isKey1 := config.GetConfig().MongoDel.Field1.Key == ""
	isValue1 := config.GetConfig().MongoDel.Field1.Value == 0

	isKey2 := config.GetConfig().MongoDel.Field2.Key == ""
	isValue2 := config.GetConfig().MongoDel.Field2.Value == 0

	isKey3 := config.GetConfig().MongoDel.Field3.Key == ""
	isValue3 := config.GetConfig().MongoDel.Field3.Value == ""

	//first, by days delete data
	if config.GetConfig().MongoDel.Days != 0 && isKey1 && isValue1 &&
		isKey2 && isValue2 && isKey3 && isValue3 {
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByDay_Panic:", err)
			panic(err)
		}

		deleteDataByDay(coll, ts)
	}

	//second, by some fields delete data
	//by three fields
	if config.GetConfig().MongoDel.Days == 0 && ((!isKey1 && !isValue1) &&
		(!isKey2 && !isValue2) && (!isKey3 && !isValue3)) {
		deleteDataByThreeFields(coll)

		return nil
	}

	//by two fields
	if config.GetConfig().MongoDel.Days == 0 && ((!isKey1 && !isValue1) &&
		(!isKey2 && !isValue2)) {
		deleteDataByTwoFields1(coll)

		return nil
	}
	if config.GetConfig().MongoDel.Days == 0 && ((!isKey2 && !isValue2) &&
		(!isKey3 && !isValue3)) {
		deleteDataByTwoFields2(coll)

		return nil
	}
	if config.GetConfig().MongoDel.Days == 0 && ((!isKey1 && !isValue1) &&
		(!isKey3 && !isValue3)) {
		deleteDataByTwoFields3(coll)

		return nil
	}

	//by one field
	if config.GetConfig().MongoDel.Days == 0 && (!isKey1 && !isValue1) {
		deleteDataByOneFields1(coll)
	}
	if config.GetConfig().MongoDel.Days == 0 && (!isKey2 && !isValue2) {
		deleteDataByOneFields2(coll)
	}
	if config.GetConfig().MongoDel.Days == 0 && (!isKey3 && !isValue3) {
		deleteDataByOneFields3(coll)
	}

	//third, by fields and days
	//by three fields and days
	if config.GetConfig().MongoDel.Days != 0 && ((!isKey1 && !isValue1) &&
		(!isKey2 && !isValue2) && (!isKey3 && !isValue3)) {
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByFieldsAndDays_Panic:", err)
			panic(err)
		}

		deleteDataByThreeFieldsAndDays(coll, ts)
		return nil
	}

	//by two fields and days
	if config.GetConfig().MongoDel.Days != 0 && ((!isKey1 && !isValue1) &&
		(!isKey2 && !isValue2)) {
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByFieldsAndDays_Panic:", err)
			panic(err)
		}

		deleteDataByTwoFieldsAndDays1(coll, ts)

		return nil
	}
	if config.GetConfig().MongoDel.Days != 0 && ((!isKey2 && !isValue2) &&
		(!isKey3 && !isValue3)) {
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByFieldsAndDays_Panic:", err)
			panic(err)
		}

		deleteDataByTwoFieldsAndDays2(coll, ts)

		return nil
	}
	if config.GetConfig().MongoDel.Days != 0 && ((!isKey1 && !isValue1) &&
		(!isKey3 && !isValue3)) {
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByFieldsAndDays_Panic:", err)
			panic(err)
		}

		deleteDataByTwoFieldsAndDays3(coll, ts)

		return nil
	}

	//by one field and days
	if config.GetConfig().MongoDel.Days != 0 && (!isKey1 && !isValue1){
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByFieldsAndDays_Panic:", err)
			panic(err)
		}

		deleteDataByOneFieldsAndDays1(coll, ts)
	}
	if config.GetConfig().MongoDel.Days != 0 && (!isKey2 && !isValue2){
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByFieldsAndDays_Panic:", err)
			panic(err)
		}

		deleteDataByOneFieldsAndDays2(coll, ts)
	}
	if config.GetConfig().MongoDel.Days != 0 && (!isKey3 && !isValue3){
		ts, err := dayToTimeStamp()
		if err != nil {
			seelog.Error("deleteDataByFieldsAndDays_Panic:", err)
			panic(err)
		}

		deleteDataByOneFieldsAndDays3(coll, ts)
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
	seelog.Info("deleteDataByDay_TS:", ts)

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

func deleteDataByOneFields1(coll *mongo.Collection) error {
	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field1.Key, config.GetConfig().MongoDel.Field1.Value},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByOneFields1 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByOneFields1:", result.DeletedCount)

	return nil
}

func deleteDataByOneFields2(coll *mongo.Collection) error {
	var isPull bool
	if config.GetConfig().MongoDel.Field2.Value == 1 {
		isPull = true
	} else if config.GetConfig().MongoDel.Field2.Value == 2 {
		isPull = false
	}

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field2.Key, isPull},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByOneFields2 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByOneFields2:", result.DeletedCount)

	return nil
}

func deleteDataByOneFields3(coll *mongo.Collection) error {
	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field3.Key, config.GetConfig().MongoDel.Field3.Value},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByOneFields3 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByOneFields3:", result.DeletedCount)

	return nil
}

func deleteDataByTwoFields1(coll *mongo.Collection) error {
	var isPull bool
	if config.GetConfig().MongoDel.Field2.Value == 1 {
		isPull = true
	} else if config.GetConfig().MongoDel.Field2.Value == 2 {
		isPull = false
	}

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field1.Key, config.GetConfig().MongoDel.Field1.Value},
			{config.GetConfig().MongoDel.Field2.Key, isPull},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByTwoFields1 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByTwoFields1:", result.DeletedCount)

	return nil
}

func deleteDataByTwoFields2(coll *mongo.Collection) error {
	var isPull bool
	if config.GetConfig().MongoDel.Field2.Value == 1 {
		isPull = true
	} else if config.GetConfig().MongoDel.Field2.Value == 2 {
		isPull = false
	}

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field2.Key, isPull},
			{config.GetConfig().MongoDel.Field3.Key, config.GetConfig().MongoDel.Field3.Value},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByTwoFields2 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByTwoFields2:", result.DeletedCount)

	return nil
}

func deleteDataByTwoFields3(coll *mongo.Collection) error {
	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field1.Key, config.GetConfig().MongoDel.Field1.Value},
			{config.GetConfig().MongoDel.Field3.Key, config.GetConfig().MongoDel.Field3.Value},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByTwoFields3 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByTwoFields3:", result.DeletedCount)

	return nil
}

func deleteDataByThreeFields(coll *mongo.Collection) error {
	var isPull bool
	if config.GetConfig().MongoDel.Field2.Value == 1 {
		isPull = true
	} else if config.GetConfig().MongoDel.Field2.Value == 2 {
		isPull = false
	}

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field1.Key, config.GetConfig().MongoDel.Field1.Value},
			{config.GetConfig().MongoDel.Field2.Key, isPull},
			{config.GetConfig().MongoDel.Field3.Key, config.GetConfig().MongoDel.Field3.Value},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByThreeFields ERR:", err)

		return err
	}

	seelog.Info("deleteDataByThreeFields:", result.DeletedCount)

	return nil
}

func deleteDataByThreeFieldsAndDays(coll *mongo.Collection, ts int64) error {
	seelog.Info("deleteDataByThreeFieldsAndDays_TS:", ts)

	var isPull bool
	if config.GetConfig().MongoDel.Field2.Value == 1 {
		isPull = true
	} else if config.GetConfig().MongoDel.Field2.Value == 2 {
		isPull = false
	}

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field1.Key, config.GetConfig().MongoDel.Field1.Value},
			{config.GetConfig().MongoDel.Field2.Key, isPull},
			{config.GetConfig().MongoDel.Field3.Key, config.GetConfig().MongoDel.Field3.Value},
			{config.GetConfig().MongoDel.TimeKey, bson.D{{"$lte", ts}}},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByThreeFieldsAndDays ERR:", err)

		return err
	}

	seelog.Info("deleteDataByThreeFieldsAndDays:", result.DeletedCount)

	return nil
}

func deleteDataByTwoFieldsAndDays1(coll *mongo.Collection, ts int64) error {
	seelog.Info("deleteDataByTwoFieldsAndDays1_TS:", ts)

	var isPull bool
	if config.GetConfig().MongoDel.Field2.Value == 1 {
		isPull = true
	} else if config.GetConfig().MongoDel.Field2.Value == 2 {
		isPull = false
	}

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field1.Key, config.GetConfig().MongoDel.Field1.Value},
			{config.GetConfig().MongoDel.Field2.Key, isPull},
			{config.GetConfig().MongoDel.TimeKey, bson.D{{"$lte", ts}}},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByTwoFieldsAndDays1 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByTwoFieldsAndDays1:", result.DeletedCount)

	return nil
}

func deleteDataByTwoFieldsAndDays2(coll *mongo.Collection, ts int64) error {
	seelog.Info("deleteDataByTwoFieldsAndDays2_TS:", ts)

	var isPull bool
	if config.GetConfig().MongoDel.Field2.Value == 1 {
		isPull = true
	} else if config.GetConfig().MongoDel.Field2.Value == 2 {
		isPull = false
	}

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field2.Key, isPull},
			{config.GetConfig().MongoDel.Field3.Key, config.GetConfig().MongoDel.Field3.Value},
			{config.GetConfig().MongoDel.TimeKey, bson.D{{"$lte", ts}}},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByTwoFieldsAndDays2 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByTwoFieldsAndDays2:", result.DeletedCount)

	return nil
}

func deleteDataByTwoFieldsAndDays3(coll *mongo.Collection, ts int64) error {
	seelog.Info("deleteDataByTwoFieldsAndDays3_TS:", ts)

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field1.Key, config.GetConfig().MongoDel.Field1.Value},
			{config.GetConfig().MongoDel.Field3.Key, config.GetConfig().MongoDel.Field3.Value},
			{config.GetConfig().MongoDel.TimeKey, bson.D{{"$lte", ts}}},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByTwoFieldsAndDays3 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByTwoFieldsAndDays3:", result.DeletedCount)

	return nil
}

func deleteDataByOneFieldsAndDays1(coll *mongo.Collection, ts int64) error {
	seelog.Info("deleteDataByOneFieldsAndDays1_TS:", ts)

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field1.Key, config.GetConfig().MongoDel.Field1.Value},
			{config.GetConfig().MongoDel.TimeKey, bson.D{{"$lte", ts}}},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByOneFieldsAndDays1 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByOneFieldsAndDays1:", result.DeletedCount)

	return nil
}

func deleteDataByOneFieldsAndDays2(coll *mongo.Collection, ts int64) error {
	seelog.Info("deleteDataByOneFieldsAndDays2_TS:", ts)

	var isPull bool
	if config.GetConfig().MongoDel.Field2.Value == 1 {
		isPull = true
	} else if config.GetConfig().MongoDel.Field2.Value == 2 {
		isPull = false
	}

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field2.Key, isPull},
			{config.GetConfig().MongoDel.TimeKey, bson.D{{"$lte", ts}}},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByOneFieldsAndDays2 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByOneFieldsAndDays2:", result.DeletedCount)

	return nil
}

func deleteDataByOneFieldsAndDays3(coll *mongo.Collection, ts int64) error {
	seelog.Info("deleteDataByOneFieldsAndDays3_TS:", ts)

	result, err := coll.DeleteMany(
		context.Background(),
		bson.D{
			{config.GetConfig().MongoDel.Field3.Key, config.GetConfig().MongoDel.Field3.Value},
			{config.GetConfig().MongoDel.TimeKey, bson.D{{"$lte", ts}}},
		},
	)
	if err != nil {
		seelog.Error("deleteDataByOneFieldsAndDays3 ERR:", err)

		return err
	}

	seelog.Info("deleteDataByOneFieldsAndDays3:", result.DeletedCount)

	return nil
}
