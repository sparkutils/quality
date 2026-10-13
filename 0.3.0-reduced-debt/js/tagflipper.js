/*
After defining a span/div with tag_list id containing li's
 */
// tag_container
// tag_list
$("#tag_list li").each(function(){

	var tag = $(this).text();
	if (tag !== "ALL") {
        $(this).click(function() {
            $(".tag_container."+tag).toggle()
        });
    } else {
        $(this).click(function() {
            $(".tag_container").toggle()
        });
    }
});
