import pandas as pd
from util import ReviewUtil

# userProvince,productSize,orderId,plusAvailable,mergeOrderStatus,recommend,imageCount,showOrderComment,days,
# usefulVoteCount,isTop,referenceTypeId,userImage,secondCategory,userLevelColor,productColor,guid,isReplyGrade,
# id,isMobile,anonymousFlag,creationTime,thirdCategory,referenceType,referenceTime,topped,viewCount,userLevelId,
# replyCount2,referenceName,videos,userClient,title,userImgFlag,firstCategory,status,userLevelName,productSales,
# uselessVoteCount,userImageUrl,integral,content,referenceImage,userClientShow,score,replies,images,discussionId,
# mobileVersion,replyCount,referenceId,nickname,afterDays

input_path = '/home/xuepo/zyj/data/aws-s3'

files = ReviewUtil.get_all_valid_path(input_path, '.csv')

df_list = []
for f in files:
    df = pd.read_csv(f, sep=',')
    try:
        print(f)
        df_sub = df[['referenceTime', 'creationTime', 'userLevelId']]
        df_list.append(df_sub)
    except KeyError:
        print("Key error, skip.")
        continue


df_total = pd.concat(df_list)

df_total['referenceTime'] = pd.to_datetime(df["referenceTime"])
df_total['creationTime'] = pd.to_datetime(df["creationTime"])
diff = df_total['creationTime'] - df_total['referenceTime']
df_total['date_diff'] = diff.apply(lambda x: x.days)
print(df_total.shape)
df_temp = df_total[['userLevelId', 'date_diff']]
xx = df_temp.groupby(['userLevelId']).sum()
print(xx)

filtered = df.loc[df['userLevelId'] == 90]
print(filtered.shape)
