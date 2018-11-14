angular.module('main').controller('SearchCtrl', ['$scope', '$rootScope', '$http', function($scope, $rootScope, $http){
    $scope.myModelId = null;
    $scope.events=[];
    $scope.clear = function() {
        $scope.myModelId = null;
    }
    var monthNames = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
      "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"
    ];

    $scope.search = function() {
        var url = "/search";
        var req = {
          method: 'POST',
          url: url,
          data:{
            "query": $scope.myModelId
          }
        };
        $http(req)
        .success(function(data, status, headers, config) {
            if(status === 200){
                for (i in data.events){
                    d = new Date(data.events[i].start_time)
                    data.events[i].time = {"month":monthNames[d.getMonth()], "day":d.getDay()}
                }
                $scope.events = data.events;
            }
        })
        .error(function(data, status, headers, config) {
          console.log("Error: "+JSON.stringify(data));
        });
    }
}])