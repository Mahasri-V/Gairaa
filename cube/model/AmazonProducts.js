
cube(`AmazonProducts`, {
  sql: `SELECT * FROM public.amazon_products`,

  measures: {
    count: {
      type: `count`,
      description: `Total number of Amazon products`
    },

    averageRating: {
      sql: `rating`,
      type: `avg`,
      description: `Average customer rating`
    },

    averageDiscount: {
      sql: `discount_percentage`,
      type: `avg`,
      description: `Average discount percentage`
    },

    averageDiscountedPrice: {
      sql: `discounted_price`,
      type: `avg`,
      description: `Average discounted price`
    },

    averageActualPrice: {
      sql: `actual_price`,
      type: `avg`,
      description: `Average actual price`
    },

    totalRatingCount: {
      sql: `rating_count`,
      type: `sum`,
      description: `Total number of recorded ratings`
    }
  },

  dimensions: {
    productId: {
      sql: `product_id`,
      type: `string`,
      primaryKey: true
    },

    productName: {
      sql: `product_name`,
      type: `string`
    },

    category: {
      sql: `category`,
      type: `string`
    },

    discountedPrice: {
      sql: `discounted_price`,
      type: `number`
    },

    actualPrice: {
      sql: `actual_price`,
      type: `number`
    },

    discountPercentage: {
      sql: `discount_percentage`,
      type: `number`
    },

    rating: {
      sql: `rating`,
      type: `number`
    },

    ratingCount: {
      sql: `rating_count`,
      type: `number`
    }
  }
});
