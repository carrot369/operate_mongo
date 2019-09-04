package mongodb

import (
	"context"
	"github.com/stretchr/testify/require"
	//"go.mongodb.org/mongo-driver/internal/testutil"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

func TestInsertExamples(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	//cs := testutil.ConnString(t)
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:12345"))
	require.NoError(t, err)
	defer client.Disconnect(ctx)

	db := client.Database("test_2")

	InsertExamples(t, db)
}

func TestDeleteExamples(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:12345"))
	require.NoError(t, err)
	defer client.Disconnect(ctx)

	db := client.Database("test_2")

	//DeleteExamples(t, db)
	DeleteRangeExamples(t,db)
}