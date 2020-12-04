from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.sql.types import *
from pyspark.sql import functions as F

plays_df_schema = StructType(
  [StructField('userId', StringType()),
   StructField('songId', StringType()),
   StructField('Plays', IntegerType())]
)

metadata_df_schema = StructType(
   [StructField('songId', StringType()),
   StructField('title', StringType()),
   StructField('release', StringType()),
   StructField('artist_name', StringType()),
   StructField('year', IntegerType())]
)

userdata = spark.read.format('csv') \
    .options(delimiter='\t', header=False, inferSchema=False) \
    .schema(plays_df_schema) \
    .load('10000.txt')

metadata = spark.read.format('csv') \
    .options(delimiter=',', header=False, inferSchema=False) \
    .schema(metadata_df_schema) \
    .load('song_data.csv')

# userdata = userdata.filter(userdata.Plays > 1)

i = 0
def id_gen(value):
    global i
    i += 1
    return i

id_func = F.udf(id_gen, IntegerType())

userId_change = userdata.select('userId').distinct().withColumn('new_userId', id_func("userId")) #.select('userId', F.monotonically_increasing_id().alias('new_userId'))

i = 0
songId_change = userdata.select('songId').distinct().withColumn('new_songId', id_func("songId")) #.select('songId', F.monotonically_increasing_id().alias('new_songId'))

unique_users = userId_change.count()
unique_songs = songId_change.count()

userdata = userdata.join(userId_change, 'userId').join(songId_change, 'songId')

# userdata.show(5)
# metadata.show(5)

rdd = userdata.rdd.map(tuple).map(lambda x: (int(x[3]), int(x[4]), int(x[2])))

model = ALS.train(rdd, 10, 5)

UserID = 13
listened_songs = userdata.filter(userdata.new_userId == UserID) \
                        .join(metadata, 'songId') \
                        .select('new_songId', 'artist_name', 'title') \
                                          
# generate list of listened songs
listened_songs_list = []
for song in listened_songs.collect():
  listened_songs_list.append(song['new_songId'])

print('Songs user has listened to:')
listened_songs.select('artist_name', 'title').show()

# generate dataframe of unlistened songs
unlistened_songs = userdata.filter(~ userdata['new_songId'].isin(listened_songs_list)) \
                            .select('new_songId').withColumn('new_UserId', F.lit(UserID)).distinct()

unlistened_songs_rdd = unlistened_songs.rdd.map(tuple).map(lambda x: (int(x[1]), int(x[0])))

# feed unlistened songs into model
predicted_listens_rdd = model.predictAll(unlistened_songs_rdd)

predicted_listens = predicted_listens_rdd.toDF(['new_userId', 'new_songId', 'rating'])

predicted_listens.join(userdata, 'new_songId') \
                 .join(metadata, 'songId') \
                 .select('artist_name', 'title', 'rating') \
                 .distinct() \
                 .orderBy('rating', ascending = False) \
                 .show(10)