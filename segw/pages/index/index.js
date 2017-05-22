var app = getApp();

Page({
  data: {
    motto: 'Goodbye World',
    userInfo: {},
  },

  onShow:function(){
    wx.stopBackgroundAudio();
  },
  
  //跳转页面函数
  jump:function(e){
    app.globalData.school=e.currentTarget.id;
    console.log(app.globalData.school);
    wx.navigateTo({
      url: '../news/news',
      success: function(res){
        
      }
    })
  },
})