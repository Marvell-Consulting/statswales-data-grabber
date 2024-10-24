//
// For guidance on how to add JavaScript see:
// https://prototype-kit.service.gov.uk/docs/adding-css-javascript-and-images
//


window.GOVUKPrototypeKit.documentReady(() => {
  // Add JavaScript here

  // Add todays date (used on homepage to show update)

  // Date
  function updateDate() {
    let today = new Date();
  
    // return number
    let dayName = today.getDay(),
      dayNum = today.getDate(),
      month = today.getMonth(),
      year = today.getFullYear();
  
    const months = [
      "January",
      "February",
      "March",
      "April",
      "May",
      "June",
      "July",
      "August",
      "September",
      "October",
      "November",
      "December",
    ];
    const dayWeek = [
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
    ];
    // value -> ID of the html element
    const IDCollection = ["day", "daynum", "month", "year"];
    // return value array with number as a index
    const val = [dayNum, months[month], year];
    for (let i = 0; i < IDCollection.length; i++) {
      document.getElementById(IDCollection[i]).firstChild.nodeValue = val[i];
    }
  }
  
  updateDate();
  

})
