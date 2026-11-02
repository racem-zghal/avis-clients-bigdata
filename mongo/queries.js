use avis
db.reviews.countDocuments()
db.inverted.countDocuments()
db.reviews.createIndex({ProductId: 1})
db.inverted.createIndex({word: 1})
db.inverted.findOne({word: "delivery"}, {word: 1, n_reviews: 1})
db.reviews.aggregate([{$match: {ProductId: "B000NMJWZO"}}, {$group: {_id: null, moy: {$avg: "$score"}, n: {$sum: 1}}}])
db.reviews.aggregate([{$group: {_id: "$sentiment_pred", n: {$sum: 1}}}])
db.reviews.find({sentiment_pred: 0, score: 1}, {summary: 1, score: 1, _id: 0}).limit(3)